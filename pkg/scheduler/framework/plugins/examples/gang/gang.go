/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gang

import (
	"fmt"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/wait"
	k8sclient "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
	batch "k8s.io/kubernetes/pkg/apis/batch"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
)

const (
	Name                         string        = "gang-scheduling-permit-plugin"
	gangAnnotation               string        = "k8s.gang.io"
	errNotGang                   string        = "pod is not part of a gang"
	errNoOwnerRef                string        = "pod does not have an owner reference"
	errNotJob                    string        = "pod is not part of a job"
	errNotFound                  string        = "gang counter key not found"
	gangWaitTimeout              time.Duration = 15 * time.Second
	iterateOverWaitingPodsPeriod time.Duration = 1 * time.Second
)

// CounterKey is the type of keys stored in GangCounter.
type CounterKey types.UID

// Countervalue is the value used in the counter to keep track of Pod UIDs.
type CounterValue map[CounterKey]struct{}

// CounterData keeps track of which pods were seen.
// It is a 2 level map, framework.Code being the first level and podId being the second level key.
type CounterData map[framework.Code]CounterValue

// GangCounter keeps track of the pod-level status per Job.
// Note: Lock must be acquired before using this type.
type GangCounter struct {
	store map[CounterKey]CounterData
	mx    sync.RWMutex
}

// Read retrieves data with the given "key" from PluginContext. If the key is not
// present an error is returned.
// Note: Lock must be acquired before using this func.
func (gc *GangCounter) Read(key CounterKey) (CounterData, error) {
	if v, ok := gc.store[key]; ok {
		return v, nil
	}
	return nil, fmt.Errorf(errNotFound)
}

// Write stores the given "val" in PluginContext with the given "key".
// Note: Lock must be acquired before using this func.
func (gc *GangCounter) Write(key CounterKey, val CounterData) {
	gc.store[key] = val
}

// Delete deletes data with the given key from PluginContext.
// Note: Lock must be acquired before using this func.
func (gc *GangCounter) Delete(key CounterKey) {
	delete(gc.store, key)
}

// Length returns the current length of the gang counter.
func (gc *GangCounter) Length() int {
	return len(gc.store)
}

// Lock acquires GangCounter lock.
func (gc *GangCounter) Lock() {
	gc.mx.Lock()
}

// Unlock releases GangCounter lock.
func (gc *GangCounter) Unlock() {
	gc.mx.Unlock()
}

// newGangCounter initializes a new GangCounter and returns it.
func newGangCounter() *GangCounter {
	return &GangCounter{
		store: make(map[CounterKey]CounterData),
	}
}

// GangSchedulingPlugin is a scheduler permit plugin that implements gang scheduling.
type GangSchedulingPlugin struct {
	fh              framework.FrameworkHandle
	iteratorStarted bool
	k8sClient       *k8sclient.Clientset
	counter         *GangCounter
}

// Name returns name of the plugin. It is used in logs, etc.
func (gsp *GangSchedulingPlugin) Name() string {
	return Name
}

var _ = framework.PermitPlugin(&GangSchedulingPlugin{})

func New(config *runtime.Unknown, fh framework.FrameworkHandle) (framework.Plugin, error) {
	k8sConfig := struct {
		Path string `json:"k8sConfigPath"`
	}{}

	err := json.Unmarshal(config.Raw, &k8sConfig)
	if err != nil {
		panic(err)
	}

	k8sClientConfig, err := buildKubeConfig(k8sConfig.Path)
	if err != nil {
		panic(err)
	}

	k8sClient, err := getKubeClient(k8sClientConfig)
	if err != nil {
		panic(err)
	}

	return &GangSchedulingPlugin{
		fh:              fh,
		iteratorStarted: false,
		k8sClient:       k8sClient,
		counter:         newGangCounter(),
	}, nil
}

// buildKubeConfig gets the client config.
func buildKubeConfig(kubeconfig string) (*restclient.Config, error) {
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig},
		&clientcmd.ConfigOverrides{}).ClientConfig()
}

func getKubeClient(config *restclient.Config) (*k8sclient.Clientset, error) {
	return k8sclient.NewForConfig(restclient.AddUserAgent(config, "gang-sched-permit-plugin"))
}

// addPodUIDToCounter adds the pod UID to a framework.Code bins for the given key (job UID).
// This is an atomic operation.
func (gsp *GangSchedulingPlugin) addPodUIDToCounter(key CounterKey, podUID CounterKey, opType framework.Code) error {
	gsp.counter.Lock()
	defer gsp.counter.Unlock()
	// Add a new counter if it does not exist for this key
	if _, e := gsp.counter.Read(CounterKey(key)); e != nil {
		gsp.counter.Write(CounterKey(key), make(CounterData))
	}
	counterData, err := gsp.counter.Read(CounterKey(key))
	if err != nil {
		return err
	}

	// Get a map for this framework.Status bin
	mapToConsider, present := counterData[opType]
	// Add a new map if not present yet
	if !present {
		counterData[opType] = make(CounterValue)
		mapToConsider = counterData[opType]
	}
	if _, present := mapToConsider[CounterKey(podUID)]; !present {
		mapToConsider[CounterKey(podUID)] = struct{}{}
	}

	gsp.counter.Write(CounterKey(key), counterData)
	return nil
}

// getCount returns the pod UIDs from all framework.Code bins for the given key (job UID).
// This is an atomic operation.
func (gsp *GangSchedulingPlugin) getCount(key CounterKey) (CounterData, error) {
	gsp.counter.Lock()
	defer gsp.counter.Unlock()
	// Add a new counter if it does not exist for this key
	if _, e := gsp.counter.Read(CounterKey(key)); e != nil {
		gsp.counter.Write(CounterKey(key), make(CounterData))
	}
	counterData, err := gsp.counter.Read(CounterKey(key))
	if err != nil {
		return nil, err
	}

	return counterData, nil
}

// getJobFromPod returns the parent job for the pod if it exists
func (gsp *GangSchedulingPlugin) getJobFromPod(pod *v1.Pod) (*batchv1.Job, error) {
	ownerReference := metav1.GetControllerOf(pod)
	if ownerReference == nil {
		klog.V(4).Infof("Owner reference of pod with ID %v is nil", pod.UID)
		return nil, fmt.Errorf(errNoOwnerRef)
	}
	// Check if it is part of a job for now.
	// Get jobClient
	batchKind := batch.Kind("Job")
	if ownerReference.Kind != batchKind.Kind {
		klog.V(4).Infof("Kind of owner reference of pod with ID %v is not a job", pod.UID)
		return nil, fmt.Errorf(errNotJob)
	}

	job, err := gsp.k8sClient.BatchV1().Jobs(pod.Namespace).Get(ownerReference.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	if job.UID != ownerReference.UID {
		return nil, fmt.Errorf("ID of job is not the same as owner reference for pod with id %v", pod.UID)
	}

	// Check annotation if this is supposed to be gang scheduled
	if _, e := job.Annotations[gangAnnotation]; !e {
		klog.V(4).Infof("pod with ID %v is not part of a gang", pod.UID)
		return nil, fmt.Errorf(errNotGang)
	}

	return job, nil
}

// cleanIfDone cleans the counter if all pods in the gang have already been scheduled
func (gsp *GangSchedulingPlugin) cleanIfDone(pod *v1.Pod) error {
	gsp.counter.Lock()
	defer gsp.counter.Unlock()
	job, err := gsp.getJobFromPod(pod)
	if err != nil {
		return nil
	}
	counterData, err := gsp.counter.Read(CounterKey(job.UID))
	if err != nil {
		return err
	}

	if int32(len(counterData[framework.Success])) == *job.Spec.Parallelism {
		// Delete this key
		gsp.counter.Delete(CounterKey(job.UID))
	}

	return nil
}

func (gsp *GangSchedulingPlugin) waitingPodsCallback(w framework.WaitingPod) {
	reject, err := gsp.shouldReject(w.GetPod())
	if err != nil || reject {
		w.Reject(err.Error())
	}

	wait, err := gsp.shouldWait(w.GetPod())
	if err != nil {
		w.Reject(err.Error())
	}

	if wait {
		return
	}

	// Check if the pod should be accepted. In this case ShouldAccept always returns true
	accept, err := gsp.shouldAccept(w.GetPod())
	if err != nil {
		w.Reject(err.Error())
	}

	if accept {
		// Clean the counter if everything in the gang is scheduled.
		gsp.cleanIfDone(w.GetPod())
		w.Allow()
	}
}

// Permit is invoked by the framework at "permit" extension point.
func (gsp *GangSchedulingPlugin) Permit(pc *framework.PluginContext, pod *v1.Pod, nodeName string) (*framework.Status, time.Duration) {
	// Start Iterating over waiting pods if this is the first time Permit is being called.
	// Note(balajismaniam): This is not the ideal place to run this. However, with the current interfaces I don't have a
	// good place to call this function.
	if gsp.counter.Length() == 0 && !gsp.iteratorStarted {
		gsp.iteratorStarted = true
		go wait.Until(func() { gsp.fh.IterateOverWaitingPods(gsp.waitingPodsCallback) }, iterateOverWaitingPodsPeriod, wait.NeverStop)
	}

	// Check if the pod should be rejected first.
	reject, err := gsp.shouldReject(pod)
	if err != nil {
		return framework.NewStatus(framework.Error, err.Error()), gangWaitTimeout
	}

	if reject {
		return framework.NewStatus(framework.Unschedulable, err.Error()), gangWaitTimeout
	}

	// Check if the pod should wait for other pods in the gang.
	wait, err := gsp.shouldWait(pod)
	if err != nil {
		return framework.NewStatus(framework.Error, err.Error()), gangWaitTimeout
	}

	if wait {
		return framework.NewStatus(framework.Wait, ""), gangWaitTimeout
	}

	// Check if the pod should be accepted. In this case ShouldAccept always returns true.
	accept, err := gsp.shouldAccept(pod)
	if err != nil {
		return framework.NewStatus(framework.Error, err.Error()), gangWaitTimeout
	}

	if accept {
		// Clean the map if everything in the gang is scheduled.
		gsp.cleanIfDone(pod)
		return framework.NewStatus(framework.Success, ""), gangWaitTimeout
	}

	msg := fmt.Sprintf("reached end of permit plugin")
	return framework.NewStatus(framework.Error, msg), gangWaitTimeout
}

// shouldReject returns true if any pod in the gang has been rejected.
func (gsp *GangSchedulingPlugin) shouldReject(pod *v1.Pod) (bool, error) {
	// For the owner referenced job, if any pod has been rejected
	// Here we assume that the same pod won't be called in shouldReject
	job, err := gsp.getJobFromPod(pod)
	if err != nil {
		// Don't return the error as we want to schedule the pod if it is not part of a job, gang or does not have an
		// owner reference.
		if err.Error() == errNotJob || err.Error() == errNotGang || err.Error() == errNoOwnerRef {
			return false, nil
		}

		return true, err
	}

	// Check if any pod has been rejected
	counterData, err := gsp.getCount(CounterKey(job.UID))
	if err != nil {
		return true, err
	}

	// if rejected > 1, return true. Else return false
	if len(counterData[framework.Unschedulable]) >= 1 {
		// Add pod to the rejected store
		err := fmt.Errorf("one of the pods in the gang was rejected")
		gsp.addPodUIDToCounter(CounterKey(job.UID), CounterKey(pod.UID), framework.Unschedulable)
		return true, err
	}

	return false, nil
}

// shouldWait returns true if any pod in the gang has not been seen yet.
func (gsp *GangSchedulingPlugin) shouldWait(pod *v1.Pod) (bool, error) {
	// Get the job
	job, err := gsp.getJobFromPod(pod)
	if err != nil {
		// Don't return the error as we want to schedule the pod if it is not part of a job, gang or does not have an
		// owner reference.
		if err.Error() == errNotJob || err.Error() == errNotGang || err.Error() == errNoOwnerRef {
			return false, nil
		}

		return false, err
	}

	counterData, err := gsp.getCount(CounterKey(job.UID))
	if err != nil {
		return false, err
	}

	// Number of pods in gang = job.Parallelism
	// if rejected == 0, waiting < #replicas, return true, else false
	if len(counterData[framework.Unschedulable]) == 0 && int32(len(counterData[framework.Wait])) < *job.Spec.Parallelism {
		// Add pod to the waiting store
		gsp.addPodUIDToCounter(CounterKey(job.UID), CounterKey(pod.UID), framework.Wait)
		return true, nil
	}

	return false, nil
}

// shouldAccept returns true in all cases.
func (gsp *GangSchedulingPlugin) shouldAccept(pod *v1.Pod) (bool, error) {
	// Get the job
	job, err := gsp.getJobFromPod(pod)
	if err != nil {
		// Don't return the error as we want to schedule the pod if it is not part of a job, gang or does not have an
		// owner reference.
		if err.Error() == errNotJob || err.Error() == errNotGang || err.Error() == errNoOwnerRef {
			return true, nil
		}

		return false, err
	}

	// Add pod to the accepted store.
	gsp.addPodUIDToCounter(CounterKey(job.UID), CounterKey(pod.UID), framework.Success)
	return true, nil
}

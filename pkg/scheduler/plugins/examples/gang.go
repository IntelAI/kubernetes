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

package examples

import (
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	batch "k8s.io/kubernetes/pkg/apis/batch"
	plugins "k8s.io/kubernetes/pkg/scheduler/plugins/v1alpha1"
)

// GangSchedulingPlugin is an example of a plugin that implements permit for gang scheduling
type GangSchedulingPlugin struct {
	backoff wait.Backoff
}

//counterAddType indicates which of the 3 bins to operate on
type counterAddType int

const (
	rejected       counterAddType = 0
	accepted       counterAddType = 1
	waiting        counterAddType = 2
	gangAnnotation string         = "k8s.gang.io"
)

//GangCounter is the map which keeps track of which pods were seen
//It is a 2 level map, counterAddType being the first level and podId being the second level key
type GangCounter struct {
	pile map[counterAddType]map[string]bool
}

// NewGangCounter initializes a new GangCounter and returns it.
func NewGangCounter() *GangCounter {
	return &GangCounter{
		pile: map[counterAddType]map[string]bool{},
	}
}

var _ = plugins.PermitPlugin(GangSchedulingPlugin{})

// Name returns name of the plugin. It is used in logs, etc.
func (mc GangSchedulingPlugin) Name() string {
	return "gang-scheduling-plugin"
}

//addPodUIDToCounter adds the pod UID to a counterAddType bin for the given key (job UID).
//This is an atomic operation.
func (mc GangSchedulingPlugin) addPodUIDToCounter(ps plugins.PluginSet, key string, podUID string, opType counterAddType) error {
	ps.Data().Ctx.Mx.RLock()
	defer ps.Data().Ctx.Mx.RUnlock()
	//Add a new counter if it does not exist for this key
	if _, e := ps.Data().Ctx.Read(plugins.ContextKey(key)); e != nil {
		ps.Data().Ctx.Write(plugins.ContextKey(key), NewGangCounter())
	}
	c, err := ps.Data().Ctx.Read(plugins.ContextKey(key))
	if err != nil {
		return err
	}
	counter := c.(*GangCounter)
	//Get a map for this counterAddType bin
	mapToConsider, present := counter.pile[opType]
	//Add a new map if not present yet
	if !present {
		counter.pile[opType] = map[string]bool{}
		mapToConsider = counter.pile[opType]
	}
	if _, present := mapToConsider[podUID]; !present {
		mapToConsider[podUID] = true
	}

	ps.Data().Ctx.Write(plugins.ContextKey(key), counter)
	return nil
}

//getCount returns the pod UIDs from all counterAddType bins for the given key (job UID).
//This is an atomic operation.
func (mc GangSchedulingPlugin) getCount(ps plugins.PluginSet, key string) (*GangCounter, error) {
	ps.Data().Ctx.Mx.RLock()
	defer ps.Data().Ctx.Mx.RUnlock()
	//Add a new counter if it does not exist for this key
	if _, e := ps.Data().Ctx.Read(plugins.ContextKey(key)); e != nil {
		ps.Data().Ctx.Write(plugins.ContextKey(key), NewGangCounter())
	}
	c, err := ps.Data().Ctx.Read(plugins.ContextKey(key))
	if err != nil {
		return nil, err
	}

	counter := c.(*GangCounter)
	return counter, nil
}

//getJobFromPod returns the parent job for the pod if it exists
func (mc GangSchedulingPlugin) getJobFromPod(ps plugins.PluginSet, pod *v1.Pod) (*batchv1.Job, error) {
	// If pod is not part of a job, return true as it's not part of a gang
	ownerReference := metav1.GetControllerOf(pod)
	if ownerReference == nil {
		return nil, fmt.Errorf("Owner reference of pod with ID %v is nil", pod.UID)
	}
	// Check if it is part of a job for now.
	// Get jobClient
	batchKind := batch.Kind("Job")

	if ownerReference.Kind != batchKind.Kind {
		return nil, fmt.Errorf("Kind of owner reference of pod with ID %v is not a job", pod.UID)
	}

	job, err := ps.Data().Client.BatchV1().Jobs(pod.Namespace).Get(ownerReference.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	if job.UID != ownerReference.UID {
		return nil, fmt.Errorf("ID of job is not the same as owner reference for pod with id %v", pod.UID)
	}

	//Check annotation if this is supposed to be gang scheduled
	if _, e := job.Annotations[gangAnnotation]; !e {
		return nil, fmt.Errorf("Job not marked to be gang scheduled")
	}

	return job, nil
}

//cleanIfDone cleans the map if all pods in the gang have already been scheduled
func (mc GangSchedulingPlugin) cleanIfDone(ps plugins.PluginSet, pod *v1.Pod) error {
	ps.Data().Ctx.Mx.RLock()
	defer ps.Data().Ctx.Mx.RUnlock()
	job, err := mc.getJobFromPod(ps, pod)
	if err != nil {
		return nil
	}
	c, err := ps.Data().Ctx.Read(plugins.ContextKey(string(job.UID)))
	if err != nil {
		return err
	}
	counter := c.(*GangCounter)

	if int32(len(counter.pile[accepted])) == *job.Spec.Parallelism {
		// Delete this key
		ps.Data().Ctx.Delete(plugins.ContextKey(string(job.UID)))
	}

	return nil
}

// Permit is invoked by the framework at "permit" extension point.
func (mc GangSchedulingPlugin) Permit(ps plugins.PluginSet, pod *v1.Pod, nodeName string) (bool, error) {

	for true {
		//Check if the pod should be rejected
		reject, err := mc.ShouldReject(ps, pod)
		if err != nil || reject {
			return false, err
		}

		//Check if the pod should wait for other pods in the gang
		wait, err := mc.ShouldWait(ps, pod)
		if err != nil {
			return false, err
		}
		if wait {
			//Sleep timeout reached
			if mc.backoff.Steps == 1 {
				return false, fmt.Errorf("Timed out waiting for the gang to be scheduled")
			}
			//Sleep with a backoff
			time.Sleep(mc.backoff.Step())
			continue
		}

		//Check if the pod should be accepted. In this case ShouldAccept always returns true
		accept, err := mc.ShouldAccept(ps, pod)
		if err != nil || !accept {
			return false, err
		}

		//Clean the map if everything in the gang is scheduled.
		mc.cleanIfDone(ps, pod)
		return true, nil
	}

	return false, nil
}

// ShouldReject returns true if any pod in the gang has been rejected.
func (mc GangSchedulingPlugin) ShouldReject(ps plugins.PluginSet, pod *v1.Pod) (bool, error) {
	// For the owner referenced job, if any pod has been rejected
	// Here we assume that the same pod won't be called in ShouldReject
	job, err := mc.getJobFromPod(ps, pod)
	if err != nil {
		// Don't return the error as we want to schedule the pod if it's not part of a job
		return false, nil
	}
	//Check if any pod has been rejected
	counter, err := mc.getCount(ps, string(job.UID))
	if err != nil {
		return false, err
	}
	// if rejected > 1, return true. Else return false
	if len(counter.pile[rejected]) >= 1 {
		// Add pod to the rejected pile
		mc.addPodUIDToCounter(ps, string(job.UID), string(pod.UID), rejected)
		return true, fmt.Errorf("One pod in the gang has been rejected")
	}

	return false, nil
}

// ShouldWait returns true if any pod in the gang has not been seen yet.
func (mc GangSchedulingPlugin) ShouldWait(ps plugins.PluginSet, pod *v1.Pod) (bool, error) {
	// Get the job
	job, err := mc.getJobFromPod(ps, pod)
	if err != nil {
		// Don't return the error as we want to schedule the pod if it's not part of a job
		return false, nil
	}
	counter, err := mc.getCount(ps, string(job.UID))
	if err != nil {
		return false, err
	}

	//Number of pods in gang = job.Parallelism
	//if rejected == 0, waiting < #replicas, return true, else false
	if len(counter.pile[rejected]) == 0 && int32(len(counter.pile[waiting])) < *job.Spec.Parallelism {
		// Add pod to the waiting pile
		mc.addPodUIDToCounter(ps, string(job.UID), string(pod.UID), waiting)
		return true, nil
	}
	return false, nil
}

// ShouldAccept returns true in all cases.
func (mc GangSchedulingPlugin) ShouldAccept(ps plugins.PluginSet, pod *v1.Pod) (bool, error) {
	// Get the job
	job, err := mc.getJobFromPod(ps, pod)
	if err != nil {
		// Don't return the error as we want to schedule the pod if it's not part of a job
		return true, nil
	}
	// Add pod to the accepted pile
	mc.addPodUIDToCounter(ps, string(job.UID), string(pod.UID), accepted)
	return true, nil
}

// NewGangSchedulingPlugin initializes a new plugin and returns it.
func NewGangSchedulingPlugin(failureThreshold int) *GangSchedulingPlugin {
	return &GangSchedulingPlugin{
		backoff: wait.Backoff{
			Duration: 1 * time.Second,
			Factor:   1, // try every second
			Steps:    failureThreshold,
		},
	}
}

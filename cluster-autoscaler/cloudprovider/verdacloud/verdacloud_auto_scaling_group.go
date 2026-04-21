/*
Copyright 2019 The Kubernetes Authors.

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

package verdacloud

import (
	"context"
	"fmt"
	"sync"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	schedulerframework "k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	klog "k8s.io/klog/v2"
)

// Asg represents an Auto Scaling Group configuration for Verdacloud instances.
type Asg struct {
	AsgRef
	minSize        int
	maxSize        int
	curSize        int
	instanceType   string
	hostnamePrefix string

	AvailabilityLocations []string

	scaleMutex sync.Mutex
}

// AsgRef is a reference to an Auto Scaling Group by name.
type AsgRef struct {
	Name string
}

// InstanceRef represents a reference to a Verdacloud instance.
type InstanceRef struct {
	ProviderID string
	Hostname   string
}

// VerdacloudNodeGroup implements cloudprovider.NodeGroup interface for Verdacloud.
type VerdacloudNodeGroup struct {
	asg     *Asg
	manager *VerdacloudManager
}

// MaxSize returns the maximum size of the node group.
func (ng *VerdacloudNodeGroup) MaxSize() int {
	return ng.asg.maxSize
}

// MinSize returns the minimum size of the node group.
func (ng *VerdacloudNodeGroup) MinSize() int {
	return ng.asg.minSize
}

// TargetSize returns the current target size of the node group.
func (ng *VerdacloudNodeGroup) TargetSize() (int, error) {
	// Safe: Register() reuses ASG pointers so they stay valid across regenerate()
	ng.manager.asgs.cacheMutex.RLock()
	size := ng.asg.curSize
	ng.manager.asgs.cacheMutex.RUnlock()
	return size, nil
}

// IncreaseSize increases the size of the node group by delta.
func (ng *VerdacloudNodeGroup) IncreaseSize(delta int) error {
	klog.Infof("IncreaseSize called for ASG %s: delta=%d", ng.asg.Name, delta)
	return ng.manager.ScaleUpAsg(ng.asg, delta)
}

// AtomicIncreaseSize is not implemented for Verdacloud.
func (ng *VerdacloudNodeGroup) AtomicIncreaseSize(delta int) error {
	return cloudprovider.ErrNotImplemented
}

// Belongs returns true if the given node belongs to this node group.
func (ng *VerdacloudNodeGroup) Belongs(node *apiv1.Node) (bool, error) {
	ref, err := instanceRefFromProviderId(node.Spec.ProviderID)
	if err != nil {
		klog.V(4).Infof("Failed to parse providerID for node %s: %v", node.Name, err)
		return false, err
	}
	targetAsg, err := ng.manager.GetAsgForInstance(ref)
	if err != nil {
		klog.V(4).Infof("Error finding ASG for hostname %s: %v", ref.Hostname, err)
		return false, err
	}
	if targetAsg == nil {
		klog.V(4).Infof("No ASG found for hostname %s", ref.Hostname)
		return false, fmt.Errorf("%s doesn't belong to a known asg", node.Name)
	}

	return targetAsg.Name == ng.asg.Name, nil
}

// DeleteNodes deletes the specified nodes from the node group.
// Part of cloudprovider.NodeGroup interface — called from CA's background
// deletion goroutines. Validates minSize and membership before delegating
// to DeleteInstances for parallel API deletion + cache cleanup.
func (ng *VerdacloudNodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	ng.manager.asgs.cacheMutex.RLock()
	currentSize := ng.asg.curSize
	minSize := ng.asg.minSize
	ng.manager.asgs.cacheMutex.RUnlock()

	if currentSize-len(nodes) < minSize {
		return fmt.Errorf("deleting %d nodes would violate min size %d (current: %d)", len(nodes), minSize, currentSize)
	}

	refs := make([]InstanceRef, 0, len(nodes))
	for _, node := range nodes {
		belongs, err := ng.Belongs(node)
		if err != nil {
			return err
		}
		if !belongs {
			return fmt.Errorf("%s belongs to a different asg than %s", node.Name, ng.asg.Name)
		}
		ref, err := instanceRefFromProviderId(node.Spec.ProviderID)
		if err != nil {
			return err
		}
		refs = append(refs, *ref)
	}

	return ng.manager.DeleteInstances(refs)
}

// ForceDeleteNodes is not implemented for Verdacloud.
func (ng *VerdacloudNodeGroup) ForceDeleteNodes(nodes []*apiv1.Node) error {
	return cloudprovider.ErrNotImplemented
}

// DecreaseTargetSize decreases the target size of the node group.
// This only reduces the in-memory target counter (curSize) to correct for
// unfulfilled capacity (e.g. instances that failed to register as nodes).
// It must NOT delete any existing nodes — actual deletions happen in DeleteNodes.
func (ng *VerdacloudNodeGroup) DecreaseTargetSize(delta int) error {
	if delta >= 0 {
		return fmt.Errorf("size decrease must be negative")
	}

	targetSize, _ := ng.TargetSize()
	newTarget := targetSize + delta
	if newTarget < ng.MinSize() {
		return fmt.Errorf("attempt to decrease target to %d, but min size is %d for ASG %s",
			newTarget, ng.MinSize(), ng.asg.Name)
	}

	klog.V(4).Infof("DecreaseTargetSize ASG %s: %d -> %d",
		ng.asg.Name, targetSize, newTarget)
	ng.manager.asgs.adjustTargetSize(ng.asg, delta)
	return nil
}

// Id returns the unique identifier of the node group.
func (ng *VerdacloudNodeGroup) Id() string { return ng.asg.Name }

// Debug returns a string representation of the node group for debugging.
func (ng *VerdacloudNodeGroup) Debug() string {
	return fmt.Sprintf("%s (%d:%d)", ng.Id(), ng.MinSize(), ng.MaxSize())
}

// Nodes returns a list of all nodes that belong to this node group.
func (ng *VerdacloudNodeGroup) Nodes() ([]cloudprovider.Instance, error) {
	return ng.manager.getInstancesForAsg(ng.asg.AsgRef)
}

// TemplateNodeInfo builds a template node for scale-up simulation.
func (ng *VerdacloudNodeGroup) TemplateNodeInfo() (*schedulerframework.NodeInfo, error) {
	ctx := context.Background()
	klog.V(4).Infof("TemplateNodeInfo called for ASG %s", ng.asg.Name)
	asgRef := AsgRef{Name: ng.asg.Name}
	template, err := ng.manager.getAsgTemplate(ctx, asgRef)
	if err != nil {
		klog.Errorf("Failed to get template for ASG %s: %v", ng.asg.Name, err)
		return nil, err
	}
	klog.V(4).Infof("Got template for ASG %s: instanceType=%s", ng.asg.Name, ng.asg.instanceType)

	node, err := ng.manager.buildNodeFromTemplate(ng.asg, template)
	if err != nil {
		klog.Errorf("Failed to build node from template for ASG %s: %v", ng.asg.Name, err)
		return nil, err
	}

	nodeInfo := schedulerframework.NewNodeInfo(node, nil)
	klog.V(4).Infof("TemplateNodeInfo created successfully for ASG %s: nodeName=%s", ng.asg.Name, node.Name)
	return nodeInfo, nil
}

// Exist returns true if the node group exists.
func (ng *VerdacloudNodeGroup) Exist() bool {
	if ng.asg == nil {
		return false
	}
	asgRef := AsgRef{Name: ng.asg.Name}
	asg, err := ng.manager.GetAsgByRef(asgRef)
	if err != nil {
		klog.V(4).Infof("Error getting ASG by ref %s: %v", asgRef.Name, err)
		return false
	}
	return asg != nil
}

// Create creates the node group on the cloud provider side.
func (ng *VerdacloudNodeGroup) Create() (cloudprovider.NodeGroup, error) {
	klog.V(4).Infof("Creating ASG: %s", ng.asg.Name)
	return ng, nil
}

// Autoprovisioned returns true if the node group is autoprovisioned.
func (ng *VerdacloudNodeGroup) Autoprovisioned() bool { return false }

// Delete deletes the node group.
func (ng *VerdacloudNodeGroup) Delete() error {
	return ng.manager.DeleteAsg(ng.asg)
}

// GetOptions returns the autoscaling options for this node group.
func (ng *VerdacloudNodeGroup) GetOptions(defaults config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	return &defaults, nil
}

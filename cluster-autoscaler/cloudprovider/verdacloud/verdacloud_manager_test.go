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
	"testing"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"

	"github.com/verda-cloud/verdacloud-sdk-go/pkg/verda"
)

// newTestManagerWithMock creates a VerdacloudManager backed by a mockDCService.
func newTestManagerWithMock(t *testing.T) (*mockDCService, *VerdacloudManager) {
	t.Helper()
	mock, asg, asgs := newTestEnvWithMock(t)
	_ = asg // asg is already in asgs.registeredAsgs
	manager := &VerdacloudManager{
		cfg:       asgs.cfg,
		dcService: mock,
		asgs:      asgs,
	}
	return mock, manager
}

func TestBuildNodeFromTemplate(t *testing.T) {
	t.Run("GPU instance", func(t *testing.T) {
		_, manager := newTestManagerWithMock(t)
		asg := &Asg{
			AsgRef:                AsgRef{Name: "gpu-workers"},
			instanceType:          "1H100.80S.22V",
			AvailabilityLocations: []string{"FIN-01", "US-01"},
		}
		template := &asgTemplate{
			InstanceType: &InstanceResource{
				InstanceType: "1H100.80S.22V",
				Arch:         "amd64",
				CPU:          22,
				Memory:       88 * 1024 * 1024 * 1024, // 88 GB
				GPU:          1,
			},
			Tags: map[string]string{"env": "prod"},
		}

		node, err := manager.buildNodeFromTemplate(asg, template)
		if err != nil {
			t.Fatalf("buildNodeFromTemplate failed: %v", err)
		}

		// Check labels
		if node.Labels["kubernetes.io/arch"] != "amd64" {
			t.Errorf("arch label: got %s", node.Labels["kubernetes.io/arch"])
		}
		if node.Labels["node.kubernetes.io/instance-type"] != "1H100.80S.22V" {
			t.Errorf("instance-type label: got %s", node.Labels["node.kubernetes.io/instance-type"])
		}
		if node.Labels[NodeGroupLabelKey] != "gpu-workers" {
			t.Errorf("node group label: got %s", node.Labels[NodeGroupLabelKey])
		}
		if node.Labels[AcceleratorLabel] != "1H100.80S.22V" {
			t.Errorf("accelerator label should be set for GPU, got %s", node.Labels[AcceleratorLabel])
		}
		if node.Labels["env"] != "prod" {
			t.Errorf("tag label: got %s", node.Labels["env"])
		}

		// Check capacity
		cpu := node.Status.Capacity[apiv1.ResourceCPU]
		if cpu.Value() != 22 {
			t.Errorf("CPU: expected 22, got %d", cpu.Value())
		}
		mem := node.Status.Capacity[apiv1.ResourceMemory]
		if mem.Value() != 88*1024*1024*1024 {
			t.Errorf("Memory: expected %d, got %d", 88*1024*1024*1024, mem.Value())
		}
		gpu := node.Status.Capacity[apiv1.ResourceName(ResourceNvidiaGPU)]
		if gpu.Value() != 1 {
			t.Errorf("GPU: expected 1, got %d", gpu.Value())
		}
		pods := node.Status.Capacity[apiv1.ResourcePods]
		if pods.Value() != 110 {
			t.Errorf("Pods: expected 110, got %d", pods.Value())
		}
	})

	t.Run("CPU instance", func(t *testing.T) {
		_, manager := newTestManagerWithMock(t)
		asg := &Asg{
			AsgRef:                AsgRef{Name: "cpu-workers"},
			instanceType:          "CPU.4V.16G",
			AvailabilityLocations: []string{"FIN-01"},
		}
		template := &asgTemplate{
			InstanceType: &InstanceResource{
				InstanceType: "CPU.4V.16G",
				Arch:         "amd64",
				CPU:          4,
				Memory:       16 * 1024 * 1024 * 1024,
				GPU:          0,
			},
			Tags: map[string]string{},
		}

		node, err := manager.buildNodeFromTemplate(asg, template)
		if err != nil {
			t.Fatalf("buildNodeFromTemplate failed: %v", err)
		}

		// CPU instance should NOT have GPU resource
		if _, exists := node.Status.Capacity[apiv1.ResourceName(ResourceNvidiaGPU)]; exists {
			t.Error("CPU instance should not have GPU resource")
		}
		// CPU instance should NOT have accelerator label
		if _, exists := node.Labels[AcceleratorLabel]; exists {
			t.Error("CPU instance should not have AcceleratorLabel")
		}

		cpu := node.Status.Capacity[apiv1.ResourceCPU]
		if cpu.Value() != 4 {
			t.Errorf("CPU: expected 4, got %d", cpu.Value())
		}
	})

	t.Run("config labels and taints applied", func(t *testing.T) {
		_, manager := newTestManagerWithMock(t)
		// Add labels and taints to config
		manager.cfg.Labels = []string{"team=ml", "tier=gpu"}
		manager.cfg.Taints = []apiv1.Taint{
			{Key: "dedicated", Value: "gpu", Effect: apiv1.TaintEffectNoSchedule},
		}

		asg := &Asg{
			AsgRef:                AsgRef{Name: testAsgName},
			instanceType:          "CPU.4V",
			AvailabilityLocations: []string{"FIN-01"},
		}
		template := &asgTemplate{
			InstanceType: &InstanceResource{InstanceType: "CPU.4V", Arch: "amd64", CPU: 4, Memory: 16 * 1024 * 1024 * 1024},
			Tags:         map[string]string{},
		}

		node, err := manager.buildNodeFromTemplate(asg, template)
		if err != nil {
			t.Fatalf("failed: %v", err)
		}
		if node.Labels["team"] != "ml" {
			t.Errorf("config label 'team' not applied: %v", node.Labels)
		}
		if len(node.Spec.Taints) != 1 || node.Spec.Taints[0].Key != "dedicated" {
			t.Errorf("config taints not applied: %v", node.Spec.Taints)
		}
	})
}

func TestGetInstancesForAsg(t *testing.T) {
	mock, manager := newTestManagerWithMock(t)
	asg := manager.asgs.getAsgs()[0]

	// Setup instances with various statuses.
	// Only "active" statuses (running, provisioning, pending) are added to the
	// cache by regenerate(). Non-active statuses (no_capacity, deleting) are
	// NOT cached and thus won't appear in getInstancesForAsg results.
	mock.setInstances([]verda.Instance{
		{ID: "1", Hostname: fmt.Sprintf("%s-vm-fin-01-00", testHostnamePrefix), Status: verda.StatusRunning, Location: "FIN-01"},
		{ID: "2", Hostname: fmt.Sprintf("%s-vm-fin-01-01", testHostnamePrefix), Status: verda.StatusProvisioning, Location: "FIN-01"},
		{ID: "3", Hostname: fmt.Sprintf("%s-vm-fin-01-02", testHostnamePrefix), Status: verda.StatusPending, Location: "FIN-01"},
	})

	// Regenerate to populate cache
	if err := manager.asgs.regenerate(); err != nil {
		t.Fatalf("regenerate failed: %v", err)
	}

	instances, err := manager.getInstancesForAsg(asg.AsgRef)
	if err != nil {
		t.Fatalf("getInstancesForAsg failed: %v", err)
	}

	// Build status map for checking
	statusByID := make(map[string]*cloudprovider.InstanceStatus)
	for _, inst := range instances {
		statusByID[inst.Id] = inst.Status
	}

	// running -> InstanceRunning
	runningID := verdacloudProviderIDPrefix + "FIN-01/" + fmt.Sprintf("%s-vm-fin-01-00", testHostnamePrefix)
	if s, ok := statusByID[runningID]; !ok || s.State != cloudprovider.InstanceRunning {
		t.Errorf("running instance should be InstanceRunning, got %v", statusByID[runningID])
	}

	// provisioning -> InstanceCreating
	provID := verdacloudProviderIDPrefix + "FIN-01/" + fmt.Sprintf("%s-vm-fin-01-01", testHostnamePrefix)
	if s, ok := statusByID[provID]; !ok || s.State != cloudprovider.InstanceCreating {
		t.Errorf("provisioning instance should be InstanceCreating, got %v", statusByID[provID])
	}

	// pending -> InstanceCreating
	pendingID := verdacloudProviderIDPrefix + "FIN-01/" + fmt.Sprintf("%s-vm-fin-01-02", testHostnamePrefix)
	if s, ok := statusByID[pendingID]; !ok || s.State != cloudprovider.InstanceCreating {
		t.Errorf("pending instance should be InstanceCreating, got %v", statusByID[pendingID])
	}

	t.Logf("getInstancesForAsg returned %d instances", len(instances))
}

func TestGetAsgTemplate(t *testing.T) {
	mock, manager := newTestManagerWithMock(t)
	asg := manager.asgs.getAsgs()[0]

	mock.instanceTypeDetails = map[string]*InstanceResource{
		testInstanceType: {
			InstanceType: testInstanceType,
			Arch:         "amd64",
			CPU:          22,
			Memory:       88 * 1024 * 1024 * 1024,
			GPU:          1,
		},
	}

	ctx := context.Background()
	template, err := manager.getAsgTemplate(ctx, asg.AsgRef)
	if err != nil {
		t.Fatalf("getAsgTemplate failed: %v", err)
	}
	if template.InstanceType.CPU != 22 {
		t.Errorf("CPU: expected 22, got %d", template.InstanceType.CPU)
	}
	if template.InstanceType.GPU != 1 {
		t.Errorf("GPU: expected 1, got %d", template.InstanceType.GPU)
	}
}

func TestVerifyCloudConfigAndPatch(t *testing.T) {
	cfg := &cloudConfig{}
	patched := verifyCloudConfigAndPatch(cfg)
	if patched.BillingConfig.Contract != "PAY_AS_YOU_GO" {
		t.Errorf("default contract: got %s", patched.BillingConfig.Contract)
	}
	if patched.BillingConfig.Price != "FIXED_PRICE" {
		t.Errorf("default price: got %s", patched.BillingConfig.Price)
	}

	// Existing values should not be overwritten
	cfg2 := &cloudConfig{}
	cfg2.BillingConfig.Contract = "CUSTOM"
	cfg2.BillingConfig.Price = "CUSTOM_PRICE"
	patched2 := verifyCloudConfigAndPatch(cfg2)
	if patched2.BillingConfig.Contract != "CUSTOM" {
		t.Errorf("should not overwrite: got %s", patched2.BillingConfig.Contract)
	}
}

func TestParseInstanceType(t *testing.T) {
	tests := []struct {
		name         string
		instanceType string
		expectCPU    int64
		expectMemory int64 // in GB before conversion
		expectGPU    int64
	}{
		{"CPU 4 core", "CPU.4V.16G", 4, 16, 0},
		{"CPU 8 core", "CPU.8V.32G", 8, 32, 0},
		{"GPU 1xH100 3-part", "1H100.80S.22V", 22, 22 * 4, 1},
		{"GPU 8xH100 3-part", "8H100.80S.176V", 176, 176 * 4, 8},
		{"GPU 2-part", "1A100.12V", 12, 12 * 4, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := parseInstanceType(tc.instanceType)
			if res.CPU != tc.expectCPU {
				t.Errorf("CPU: expected %d, got %d", tc.expectCPU, res.CPU)
			}
			expectedMem := tc.expectMemory * 1024 * 1024 * 1024
			if res.Memory != expectedMem {
				t.Errorf("Memory: expected %d, got %d", expectedMem, res.Memory)
			}
			if res.GPU != tc.expectGPU {
				t.Errorf("GPU: expected %d, got %d", tc.expectGPU, res.GPU)
			}
		})
	}
}

func TestRefresh(t *testing.T) {
	mock, manager := newTestManagerWithMock(t)
	mock.setInstances([]verda.Instance{})

	// First call should refresh
	err := manager.Refresh()
	if err != nil {
		t.Fatalf("first Refresh failed: %v", err)
	}

	// Second call within refreshInterval should be throttled (no-op)
	mock.instancesErr = fmt.Errorf("should not be called")
	err = manager.Refresh()
	if err != nil {
		t.Errorf("throttled Refresh should not error: %v", err)
	}

	// After interval expires, should refresh again
	manager.lastRefresh = time.Now().Add(-2 * refreshInterval)
	mock.instancesErr = nil
	mock.setInstances([]verda.Instance{})
	err = manager.Refresh()
	if err != nil {
		t.Errorf("expired Refresh should work: %v", err)
	}
}

func TestGetAvailableGPUTypes(t *testing.T) {
	mock, manager := newTestManagerWithMock(t)
	mock.instanceTypes = []string{"1H100.80S.22V", "CPU.4V.16G", "8H100.80S.176V", "CPU.8V.32G"}

	gpuTypes := manager.GetAvailableGPUTypes()
	if len(gpuTypes) != 2 {
		t.Errorf("expected 2 GPU types, got %d: %v", len(gpuTypes), gpuTypes)
	}
	if _, ok := gpuTypes["1H100.80S.22V"]; !ok {
		t.Error("missing 1H100.80S.22V")
	}
	if _, ok := gpuTypes["8H100.80S.176V"]; !ok {
		t.Error("missing 8H100.80S.176V")
	}
	// CPU types should be filtered out
	if _, ok := gpuTypes["CPU.4V.16G"]; ok {
		t.Error("CPU type should be filtered out")
	}
}

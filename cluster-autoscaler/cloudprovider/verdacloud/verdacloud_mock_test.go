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
	"strings"
	"sync"

	"github.com/verda-cloud/verdacloud-sdk-go/pkg/verda"
)

// instanceAction records an PerformInstanceAction call (instance ID + action name).
type instanceAction struct {
	ID     string
	Action string
}

// mockDCService implements dcService for unit tests.
// All fields are protected by mu for safe use in parallel test goroutines.
type mockDCService struct {
	mu sync.Mutex

	// instances is the mock API response for ListInstancesCached.
	instances    []verda.Instance
	instancesErr error

	// Callback hooks — when non-nil, override the default behaviour.
	createFunc func(ctx context.Context, req *verda.CreateInstanceRequest) (*verda.Instance, error)
	actionFunc func(ctx context.Context, instanceID, action string) error
	deleteFunc func(ctx context.Context, instanceID string) error

	// Recorded calls for assertions.
	created []verda.CreateInstanceRequest
	actions []instanceAction

	// GetInstanceAvailabilityLocation controls.
	availLocation    string
	availLocationErr error

	// CreateStartScript controls.
	startScriptID  string
	startScriptErr error

	// ListInstanceTypes / GetInstanceTypeDetails controls.
	instanceTypes       []string
	instanceTypeDetails map[string]*InstanceResource
}

// setInstances replaces the mock instance list in a thread-safe manner.
func (m *mockDCService) setInstances(instances []verda.Instance) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.instances = make([]verda.Instance, len(instances))
	copy(m.instances, instances)
}

func (m *mockDCService) ListInstancesCached(_ context.Context) ([]verda.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.instancesErr != nil {
		return nil, m.instancesErr
	}
	result := make([]verda.Instance, len(m.instances))
	copy(result, m.instances)
	return result, nil
}

func (m *mockDCService) GetInstanceByHostname(ctx context.Context, hostname string) (*verda.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.instancesErr != nil {
		return nil, m.instancesErr
	}
	for _, inst := range m.instances {
		if inst.Hostname == hostname && isActiveStatus(inst.Status) {
			return &inst, nil
		}
	}
	return nil, fmt.Errorf("instance %s not found", hostname)
}

func (m *mockDCService) GetActiveInstancesForAsg(_ context.Context, asgName string, hostnamePrefix ...string) ([]verda.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.instancesErr != nil {
		return nil, m.instancesErr
	}

	matchKey := asgName
	if len(hostnamePrefix) > 0 && hostnamePrefix[0] != "" {
		matchKey = hostnamePrefix[0]
	}

	var filtered []verda.Instance
	for _, inst := range m.instances {
		if !isActiveStatus(inst.Status) {
			continue
		}
		extractedPrefix, err := extractAsgNameFromHostname(inst.Hostname)
		if err != nil {
			continue
		}
		if strings.EqualFold(extractedPrefix, matchKey) {
			filtered = append(filtered, inst)
		}
	}
	return filtered, nil
}

func (m *mockDCService) CreateInstance(ctx context.Context, req *verda.CreateInstanceRequest) (*verda.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.created = append(m.created, *req)

	if m.createFunc != nil {
		return m.createFunc(ctx, req)
	}

	// Default: return a mock instance with predictable ID.
	return &verda.Instance{
		ID:       fmt.Sprintf("mock-id-%s", req.Hostname),
		Hostname: req.Hostname,
		Status:   verda.StatusOrdered,
	}, nil
}

func (m *mockDCService) DeleteInstance(ctx context.Context, instanceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, instanceID)
	}
	return nil
}

func (m *mockDCService) PerformInstanceAction(ctx context.Context, instanceID, action string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.actions = append(m.actions, instanceAction{ID: instanceID, Action: action})

	if m.actionFunc != nil {
		return m.actionFunc(ctx, instanceID, action)
	}
	return nil
}

func (m *mockDCService) GetInstanceAvailabilityLocation(_ context.Context, _ string, _ []string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.availLocation, m.availLocationErr
}

func (m *mockDCService) CreateStartScript(_ context.Context, name, _ string) (*verda.StartupScript, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startScriptErr != nil {
		return nil, m.startScriptErr
	}
	return &verda.StartupScript{
		ID:   m.startScriptID,
		Name: name,
	}, nil
}

func (m *mockDCService) DeleteStartScript(_ context.Context, _ string) error {
	return nil
}

func (m *mockDCService) ListInstanceTypes(_ context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.instanceTypes, nil
}

func (m *mockDCService) GetInstanceTypeDetails(_ context.Context, instanceType string) (*InstanceResource, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.instanceTypeDetails != nil {
		if details, ok := m.instanceTypeDetails[instanceType]; ok {
			return details, nil
		}
	}
	return nil, fmt.Errorf("instance type %s not found", instanceType)
}

func (m *mockDCService) InvalidateCache() {
	// no-op for tests
}

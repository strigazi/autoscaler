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
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/verda-cloud/verdacloud-sdk-go/pkg/verda"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube_client "k8s.io/client-go/kubernetes"
	klog "k8s.io/klog/v2"
)

// ASG constants for hostname parsing and capacity management.
const (
	// ASG_SEPARATOR_MAGIC_NUMBER is the magic string used in hostname format: {prefix}-vm-{location}-{random}
	ASG_SEPARATOR_MAGIC_NUMBER = "vm"

	FAILED_INSTANCE_BACKOFF_DURATION  = 5 * time.Minute  // wait before retrying after terminal failure
	FAILED_INSTANCE_CLEANUP_AGE       = 10 * time.Minute // delete stuck failed instances after this
	FAILED_INSTANCE_MAP_ENTRY_TTL     = 1 * time.Hour    // prevent unbounded map growth
	MAX_CONCURRENT_INSTANCE_CREATIONS = 10
	// NODE_SWEEP_TIMEOUT bounds the K8s API calls used to reap orphan Nodes so
	// a slow apiserver can't stall the refresh loop.
	NODE_SWEEP_TIMEOUT = 10 * time.Second
)

// ASG_SEPARATOR is the separator pattern used to parse hostnames.
var ASG_SEPARATOR = fmt.Sprintf("-%s-", ASG_SEPARATOR_MAGIC_NUMBER)

type autoScalingGroups struct {
	registeredAsgs    map[AsgRef]*Asg
	asgToInstances    map[AsgRef][]InstanceRef
	instanceToAsg     map[InstanceRef]*Asg
	instanceIDs       map[InstanceRef]string // ref → VerdaCloud API instance ID
	asgNodeGroupSpecs map[AsgRef]string
	cfg               *cloudConfig
	dcService         dcService
	// kubeClient reaps orphan K8s Node objects whose VerdaCloud VMs have
	// disappeared from the API. Nil in unit tests — sweep is a no-op then.
	kubeClient kube_client.Interface

	failedInstances  map[string]time.Time // tracks failed instances (no_capacity, error, unknown) for backoff
	lastFailureCheck map[AsgRef]time.Time

	cacheMutex sync.RWMutex
}

func newAutoScalingGroups(dcService dcService, nodeGroupSpecs []string, cfg *cloudConfig, kubeClient kube_client.Interface) (*autoScalingGroups, error) {
	registry := &autoScalingGroups{
		registeredAsgs:    make(map[AsgRef]*Asg),
		asgToInstances:    make(map[AsgRef][]InstanceRef),
		instanceToAsg:     make(map[InstanceRef]*Asg),
		instanceIDs:       make(map[InstanceRef]string),
		asgNodeGroupSpecs: make(map[AsgRef]string),
		failedInstances:   make(map[string]time.Time),
		lastFailureCheck:  make(map[AsgRef]time.Time),
		cfg:               cfg,
		dcService:         dcService,
		kubeClient:        kubeClient,
	}

	if err := registry.parseASGNodeGroupSpecs(nodeGroupSpecs); err != nil {
		return nil, err
	}

	return registry, nil
}

func (m *autoScalingGroups) getAsgs() []*Asg {
	m.cacheMutex.RLock()
	defer m.cacheMutex.RUnlock()
	asgs := make([]*Asg, 0, len(m.registeredAsgs))
	for _, asg := range m.registeredAsgs {
		asgs = append(asgs, asg)
	}
	return asgs
}

func (m *autoScalingGroups) GetAsgByRef(ref AsgRef) (*Asg, error) {
	m.cacheMutex.RLock()
	defer m.cacheMutex.RUnlock()
	asg, exists := m.registeredAsgs[ref]
	if !exists {
		return nil, fmt.Errorf("ASG not found for ref: %s", ref.Name)
	}
	return asg, nil
}


func (m *autoScalingGroups) FindASGForInstance(ref *InstanceRef) (*Asg, error) {
	m.cacheMutex.RLock()
	defer m.cacheMutex.RUnlock()

	if asg, exists := m.instanceToAsg[*ref]; exists {
		return asg, nil
	}
	// ProviderID format can vary; try hostname match
	for cachedRef, asg := range m.instanceToAsg {
		if cachedRef.Hostname == ref.Hostname {
			return asg, nil
		}
	}
	klog.V(4).Infof("Instance %s not found in cache", ref.Hostname)
	return nil, nil
}

// findCachedRefByHostname requires cacheMutex held.
func (m *autoScalingGroups) findCachedRefByHostname(hostname string) (*InstanceRef, *Asg) {
	for ref, asg := range m.instanceToAsg {
		if ref.Hostname == hostname {
			return &ref, asg
		}
	}
	return nil, nil
}

func (m *autoScalingGroups) regenerate() error {
	ctx := context.Background()

	// 1. Fetch current VM state from API
	allInstances, err := m.dcService.ListInstancesCached(ctx)
	if err != nil {
		return fmt.Errorf("failed to list instances: %w", err)
	}

	// 2. Snapshot current cache state under read lock
	existingAsgs, existingAsgToInstances, existingInstanceIDs, asgCurSizes := func() (map[AsgRef]*Asg, map[AsgRef][]InstanceRef, map[InstanceRef]string, map[AsgRef]int) {
		m.cacheMutex.RLock()
		defer m.cacheMutex.RUnlock()
		ea := make(map[AsgRef]*Asg, len(m.registeredAsgs))
		cs := make(map[AsgRef]int, len(m.registeredAsgs))
		for ref, asg := range m.registeredAsgs {
			ea[ref] = asg
			cs[ref] = asg.curSize
		}
		eai := make(map[AsgRef][]InstanceRef, len(m.asgToInstances))
		for ref, instances := range m.asgToInstances {
			eai[ref] = append([]InstanceRef(nil), instances...)
		}
		eids := make(map[InstanceRef]string, len(m.instanceIDs))
		for ref, id := range m.instanceIDs {
			eids[ref] = id
		}
		return ea, eai, eids, cs
	}()

	// 3. Rebuild instance maps from API state
	newCache := &instanceMaps{
		instanceToAsg:  make(map[InstanceRef]*Asg),
		asgToInstances: make(map[AsgRef][]InstanceRef),
		instanceIDs:    make(map[InstanceRef]string),
	}
	allFailedInstances := make(map[AsgRef][]verda.Instance)

	oldCache := &instanceMaps{
		asgToInstances: existingAsgToInstances,
		instanceIDs:    existingInstanceIDs,
	}

	for _, asg := range existingAsgs {
		activeInstances, failedInstances, apiSeenHostnames := m.categorizeInstancesForAsg(allInstances, asg)
		allFailedInstances[asg.AsgRef] = failedInstances

		for _, inst := range activeInstances {
			ref := InstanceRef{
				Hostname:   inst.Hostname,
				ProviderID: verdacloudProviderIDPrefix + inst.Location + "/" + inst.Hostname,
			}
			newCache.addInstance(ref, asg, inst.ID)
		}

		m.preserveCachedInstances(asg, asgCurSizes[asg.AsgRef], apiSeenHostnames, oldCache, newCache)
	}

	// 4. Swap cache and reconcile curSize atomically
	m.cacheMutex.Lock()
	m.instanceToAsg = newCache.instanceToAsg
	m.asgToInstances = newCache.asgToInstances
	m.instanceIDs = newCache.instanceIDs
	m.reconcileCurSize(newCache.asgToInstances, allFailedInstances)
	m.cacheMutex.Unlock()

	// 5. Handle failed instances (backoff tracking, cleanup)
	m.processFailedInstances(existingAsgs, allFailedInstances)

	// 6. Reap K8s Node objects whose VerdaCloud VMs are gone.
	//    Since there is no cloud-controller-manager for VerdaCloud, nothing else
	//    deletes orphan Nodes — without this sweep they linger as NotReady forever.
	apiHostnames := make(map[string]bool, len(allInstances))
	for _, inst := range allInstances {
		apiHostnames[inst.Hostname] = true
	}
	m.sweepOrphanNodes(ctx, apiHostnames)

	return nil
}

// sweepOrphanNodes deletes K8s Node objects whose VerdaCloud VM has disappeared
// from the API. Only touches Nodes whose hostname was created by one of our
// registered ASGs, so manually-provisioned VerdaCloud VMs (e.g. control plane)
// are left alone. Best-effort: errors are logged, never fail the refresh loop.
func (m *autoScalingGroups) sweepOrphanNodes(ctx context.Context, apiHostnames map[string]bool) {
	if m.kubeClient == nil {
		return
	}

	listCtx, cancel := context.WithTimeout(ctx, NODE_SWEEP_TIMEOUT)
	defer cancel()
	nodes, err := m.kubeClient.CoreV1().Nodes().List(listCtx, metav1.ListOptions{})
	if err != nil {
		klog.Warningf("sweepOrphanNodes: failed to list Nodes: %v", err)
		return
	}

	for i := range nodes.Items {
		node := &nodes.Items[i]
		pid := node.Spec.ProviderID
		if !strings.HasPrefix(pid, verdacloudProviderIDPrefix) {
			continue
		}
		ref, err := instanceRefFromProviderId(pid)
		if err != nil {
			klog.V(4).Infof("sweepOrphanNodes: cannot parse providerID %q on Node %s: %v", pid, node.Name, err)
			continue
		}
		if !m.belongsToManagedAsg(ref.Hostname) {
			continue
		}
		if apiHostnames[ref.Hostname] {
			continue
		}

		deleteCtx, cancel := context.WithTimeout(ctx, NODE_SWEEP_TIMEOUT)
		err = m.kubeClient.CoreV1().Nodes().Delete(deleteCtx, node.Name, metav1.DeleteOptions{})
		cancel()
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			klog.Warningf("sweepOrphanNodes: failed to delete orphan Node %s (hostname %s): %v", node.Name, ref.Hostname, err)
			continue
		}
		klog.Infof("sweepOrphanNodes: deleted orphan Node %s (hostname %s) — VerdaCloud VM no longer present", node.Name, ref.Hostname)
	}
}

// belongsToManagedAsg returns true when the hostname matches the hostnamePrefix
// (or Name fallback) of any registered ASG. Mirrors the matching rule used in
// categorizeInstancesForAsg so the sweep scope is the same as the cache scope.
func (m *autoScalingGroups) belongsToManagedAsg(hostname string) bool {
	prefix, err := extractAsgNameFromHostname(hostname)
	if err != nil {
		return false
	}
	m.cacheMutex.RLock()
	defer m.cacheMutex.RUnlock()
	for _, asg := range m.registeredAsgs {
		matchKey := asg.hostnamePrefix
		if matchKey == "" {
			matchKey = asg.Name
		}
		if strings.EqualFold(prefix, matchKey) {
			return true
		}
	}
	return false
}

// reconcileCurSize adjusts curSize for each ASG based on active and failed instance counts.
// Must be called with cacheMutex held.
//
// Rules:
//   - If active > curSize: increase (new instances appeared, e.g. manual creation)
//   - If active < curSize AND failures detected: decrease (failed instances won't become nodes)
//   - Otherwise: keep curSize (optimistic count from scale-up, instances still provisioning)
func (m *autoScalingGroups) reconcileCurSize(asgToInstances map[AsgRef][]InstanceRef, failedByAsg map[AsgRef][]verda.Instance) {
	for ref, asg := range m.registeredAsgs {
		activeCount := len(asgToInstances[ref])
		failedCount := len(failedByAsg[ref])

		if activeCount > asg.curSize {
			klog.V(4).Infof("ASG %s curSize: %d -> %d (active instances increased)",
				asg.Name, asg.curSize, activeCount)
			asg.curSize = activeCount
		} else if failedCount > 0 && asg.curSize > activeCount {
			klog.Warningf("ASG %s: curSize %d -> %d (detected %d failed instances)",
				asg.Name, asg.curSize, activeCount, failedCount)
			asg.curSize = activeCount
		}
	}
}

// instanceMaps groups the instance maps built or snapshotted during regenerate().
type instanceMaps struct {
	instanceToAsg  map[InstanceRef]*Asg
	asgToInstances map[AsgRef][]InstanceRef
	instanceIDs    map[InstanceRef]string
}

func (c *instanceMaps) addInstance(ref InstanceRef, asg *Asg, instanceID string) {
	c.instanceToAsg[ref] = asg
	c.asgToInstances[asg.AsgRef] = append(c.asgToInstances[asg.AsgRef], ref)
	if instanceID != "" {
		c.instanceIDs[ref] = instanceID
	}
}

// categorizeInstancesForAsg categorizes instances belonging to an ASG into active, failed,
// and returns all hostnames seen in the API for this ASG (including ignored statuses like
// offline, deleting). The seenHostnames set is used by preserveCachedInstances to distinguish
// "not in API yet" from "in API but not active."
func (m *autoScalingGroups) categorizeInstancesForAsg(allInstances []verda.Instance, asg *Asg) (active, failed []verda.Instance, seenHostnames map[string]bool) {
	matchKey := asg.hostnamePrefix
	if matchKey == "" {
		matchKey = asg.Name
	}

	seenHostnames = make(map[string]bool)
	for _, inst := range allInstances {
		prefix, err := extractAsgNameFromHostname(inst.Hostname)
		if err != nil || !strings.EqualFold(prefix, matchKey) {
			continue
		}
		seenHostnames[inst.Hostname] = true
		if isActiveStatus(inst.Status) {
			active = append(active, inst)
		} else if isProvisioningFailedStatus(inst.Status) {
			failed = append(failed, inst)
		}
	}
	return
}

// preserveCachedInstances keeps provisioning instances that haven't appeared in API yet.
// snapshotCurSize is the curSize captured under lock at the start of regenerate().
// apiSeenHostnames contains ALL hostnames seen in the API response (active + failed + ignored),
// so we only preserve instances that are truly not yet visible to the API.
func (m *autoScalingGroups) preserveCachedInstances(asg *Asg, snapshotCurSize int, apiSeenHostnames map[string]bool, old *instanceMaps, new *instanceMaps) {
	if len(new.asgToInstances[asg.AsgRef]) >= snapshotCurSize {
		return
	}

	for _, existingRef := range old.asgToInstances[asg.AsgRef] {
		if len(new.asgToInstances[asg.AsgRef]) >= snapshotCurSize {
			break
		}
		if !apiSeenHostnames[existingRef.Hostname] {
			id := old.instanceIDs[existingRef]
			new.addInstance(existingRef, asg, id)
		}
	}
}

// isProvisioningFailedStatus returns true if the status indicates the instance
// failed to provision. These statuses can occur after CreateInstance API succeeds
// but the instance fails during provisioning.
func isProvisioningFailedStatus(status string) bool {
	switch strings.ToLower(status) {
	case verda.StatusNoCapacity, verda.StatusError, verda.StatusUnknown:
		return true
	}
	return false
}

// processFailedInstances handles failed instances using pre-categorized data from regenerate.
// It tracks failures for backoff and cleans up old failed instances.
func (m *autoScalingGroups) processFailedInstances(registeredAsgs map[AsgRef]*Asg, failedByAsg map[AsgRef][]verda.Instance) {
	now := time.Now()

	// Clean up old entries from tracking map
	m.cacheMutex.Lock()
	for hostname, markedTime := range m.failedInstances {
		if now.Sub(markedTime) > FAILED_INSTANCE_MAP_ENTRY_TTL {
			delete(m.failedInstances, hostname)
		}
	}
	m.cacheMutex.Unlock()

	for asgRef, asg := range registeredAsgs {
		failedInstances := failedByAsg[asgRef]
		if len(failedInstances) == 0 {
			continue
		}

		for _, inst := range failedInstances {
			m.trackAndCleanupFailedInstance(inst, now)
		}

		m.cacheMutex.Lock()
		m.lastFailureCheck[asgRef] = now
		m.cacheMutex.Unlock()
		klog.Warningf("ASG %s has %d failed instances, backoff until %v", asg.Name, len(failedInstances), now.Add(FAILED_INSTANCE_BACKOFF_DURATION))
	}
}

// trackAndCleanupFailedInstance tracks the failed instance and deletes it if it's been failed too long.
func (m *autoScalingGroups) trackAndCleanupFailedInstance(inst verda.Instance, now time.Time) {
	markedTime, exists := func() (time.Time, bool) {
		m.cacheMutex.Lock()
		defer m.cacheMutex.Unlock()
		t, ok := m.failedInstances[inst.Hostname]
		if !ok {
			m.failedInstances[inst.Hostname] = now
		}
		return t, ok
	}()
	if !exists {
		klog.Warningf("Found failed instance: %s (status: %s)", inst.Hostname, inst.Status)
		return
	}

	if now.Sub(markedTime) > FAILED_INSTANCE_CLEANUP_AGE {
		ctx := context.Background()
		if err := m.dcService.DeleteInstance(ctx, inst.ID); err != nil {
			klog.Errorf("Failed to delete failed instance %s: %v", inst.Hostname, err)
		} else {
			m.cacheMutex.Lock()
			delete(m.failedInstances, inst.Hostname)
			m.cacheMutex.Unlock()
			klog.Infof("Deleted old failed instance %s (status was: %s)", inst.Hostname, inst.Status)
		}
	}
}

func (m *autoScalingGroups) buildASGFromSpec(spec string) (*Asg, error) {
	asgSpec, err := parseAsgSpec(spec)
	if err != nil {
		return nil, err
	}

	nodeConfig := m.cfg.GetNodeConfig(asgSpec.name)
	return &Asg{
		AsgRef:                AsgRef{Name: asgSpec.name},
		minSize:               asgSpec.minSize,
		maxSize:               asgSpec.maxSize,
		instanceType:          strings.ToUpper(asgSpec.instanceType),
		hostnamePrefix:        asgSpec.hostnamePrefix,
		AvailabilityLocations: nodeConfig.AvailableLocations,
	}, nil
}

func (m *autoScalingGroups) parseASGNodeGroupSpecs(specs []string) error {
	for _, spec := range specs {
		asg, err := m.buildASGFromSpec(spec)
		if err != nil {
			return err
		}
		m.registeredAsgs[asg.AsgRef] = asg
		m.asgNodeGroupSpecs[asg.AsgRef] = spec
	}
	klog.V(4).Infof("Registered %d ASGs", len(m.asgNodeGroupSpecs))
	return nil
}

func (m *autoScalingGroups) incrementTargetSize(asg *Asg, delta int) (int, error) {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()

	if asg.curSize+delta > asg.maxSize {
		return 0, fmt.Errorf("size increase is too large - desired:%d max:%d",
			asg.curSize+delta, asg.maxSize)
	}

	asg.curSize += delta
	return asg.curSize, nil
}

func (m *autoScalingGroups) adjustTargetSize(asg *Asg, delta int) {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()
	asg.curSize += delta
}

// instanceCreateResult pairs an InstanceRef with the VerdaCloud API instance ID
// returned from the create response.
type instanceCreateResult struct {
	ref InstanceRef
	id  string
}

func (m *autoScalingGroups) updateCacheWithInstances(asg *Asg, results []instanceCreateResult) {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()

	for _, r := range results {
		m.instanceToAsg[r.ref] = asg
		m.asgToInstances[asg.AsgRef] = append(m.asgToInstances[asg.AsgRef], r.ref)
		m.instanceIDs[r.ref] = r.id
	}
}

func (m *autoScalingGroups) scaleUpAsg(asg *Asg, delta int) error {
	if delta <= 0 {
		return fmt.Errorf("size increase must be positive")
	}

	ctx := context.Background()

	asg.scaleMutex.Lock()
	defer asg.scaleMutex.Unlock()

	klog.Infof("Scale-up ASG %s by %d (curSize=%d, max=%d)", asg.Name, delta, asg.curSize, asg.maxSize)

	// Check no_capacity backoff
	if err := m.checkFailureBackoff(asg); err != nil {
		return err
	}

	newSize, err := m.incrementTargetSize(asg, delta)
	if err != nil {
		return err
	}

	rollbackNeeded := true
	defer func() {
		if rollbackNeeded {
			m.adjustTargetSize(asg, -delta)
		}
	}()

	location, err := m.dcService.GetInstanceAvailabilityLocation(ctx, asg.instanceType, asg.AvailabilityLocations)
	if err != nil {
		return fmt.Errorf("availability check failed: %w", err)
	}
	if location == "" {
		return fmt.Errorf("instance type %s not available", asg.instanceType)
	}

	nodeConfig, err := m.getNodeConfigForAsg(asg)
	if err != nil {
		return fmt.Errorf("get node config failed: %w", err)
	}

	rollbackNeeded = false // partial success is ok from here on

	results, errs := m.createInstances(ctx, asg, nodeConfig, location, delta)
	if len(results) > 0 {
		m.updateCacheWithInstances(asg, results)
	}

	failedCount := delta - len(results)
	if failedCount > 0 {
		m.adjustTargetSize(asg, -failedCount)
	}

	klog.Infof("Scale-up ASG %s complete: created %d/%d instances (curSize=%d)", asg.Name, len(results), delta, newSize)

	if len(errs) > 0 {
		return fmt.Errorf("failed to create %d/%d instances: %w", len(errs), delta, errors.Join(errs...))
	}
	return nil
}

func (m *autoScalingGroups) checkFailureBackoff(asg *Asg) error {
	m.cacheMutex.RLock()
	lastCheck, exists := m.lastFailureCheck[asg.AsgRef]
	m.cacheMutex.RUnlock()

	if exists && time.Since(lastCheck) < FAILED_INSTANCE_BACKOFF_DURATION {
		remaining := FAILED_INSTANCE_BACKOFF_DURATION - time.Since(lastCheck)
		return fmt.Errorf("failure backoff active for %v", remaining)
	}
	return nil
}

func (m *autoScalingGroups) createInstances(ctx context.Context, asg *Asg, nodeConfig *nodeConfig, location string, count int) ([]instanceCreateResult, []error) {
	sem := make(chan struct{}, MAX_CONCURRENT_INSTANCE_CREATIONS)
	var wg sync.WaitGroup
	resultsCh := make(chan instanceCreateResult, count)
	errsCh := make(chan error, count)

	for i := 0; i < count; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			instanceID, hostname, err := m.createInstanceForAsg(ctx, asg, nodeConfig, location)
			if err != nil {
				errsCh <- err
				return
			}
			resultsCh <- instanceCreateResult{
				ref: InstanceRef{
					Hostname:   hostname,
					ProviderID: verdacloudProviderIDPrefix + location + "/" + hostname,
				},
				id: instanceID,
			}
		}()
	}

	wg.Wait()
	close(resultsCh)
	close(errsCh)

	var results []instanceCreateResult
	var errs []error
	for r := range resultsCh {
		results = append(results, r)
	}
	for err := range errsCh {
		errs = append(errs, err)
	}
	return results, errs
}

func (m *autoScalingGroups) getNodeConfigForAsg(asg *Asg) (*nodeConfig, error) {
	nodeConfig := m.cfg.GetNodeConfig(asg.Name)

	isGPU := isGPUInstanceType(asg.instanceType)
	if isGPU {
		nodeConfig.Image = m.cfg.Image.GPU
	} else {
		nodeConfig.Image = m.cfg.Image.CPU
	}

	return nodeConfig, nil
}

func (m *autoScalingGroups) createInstanceForAsg(ctx context.Context, asg *Asg, nodeConfig *nodeConfig, location string) (string, string, error) {
	baseName := asg.hostnamePrefix
	if baseName == "" {
		baseName = asg.Name
	}

	hostname := strings.ReplaceAll(
		fmt.Sprintf("%s%s%s-%08x", baseName, ASG_SEPARATOR, strings.ToLower(location), rand.Uint32()),
		".", "-")

	providerID := fmt.Sprintf("verdacloud://%s/%s", location, hostname)
	klog.V(4).Infof("Creating instance %s with providerID=%s", hostname, providerID)

	startupScriptID, err := m.createOrGetStartupScript(ctx, asg, nodeConfig, providerID)
	if err != nil {
		return "", hostname, fmt.Errorf("create startup script failed: %w", err)
	}
	if startupScriptID == "" {
		return "", hostname, errors.New("startup script creation returned empty ID")
	}
	defer m.dcService.DeleteStartScript(ctx, startupScriptID)

	if nodeConfig.Image == "" {
		return "", hostname, fmt.Errorf("no image configured for instance type %s", asg.instanceType)
	}

	input := verda.CreateInstanceRequest{
		InstanceType:    asg.instanceType,
		Image:           nodeConfig.Image,
		SSHKeyIDs:       nodeConfig.SSHKeyIDs,
		StartupScriptID: &startupScriptID,
		Hostname:        hostname,
		Description:     asg.Name,
		LocationCode:    location,
		IsSpot:          nodeConfig.IsSpot,
		Contract:        nodeConfig.Contract,
		Pricing:         nodeConfig.Price,
		OSVolume: &verda.OSVolumeCreateRequest{
			Name: fmt.Sprintf("%s-os-volume", hostname),
			Size: nodeConfig.OSVolumeSize,
		},
	}

	if len(nodeConfig.Volumes) > 0 {
		input.Volumes = make([]verda.VolumeCreateRequest, len(nodeConfig.Volumes))
		for i, vol := range nodeConfig.Volumes {
			input.Volumes[i] = verda.VolumeCreateRequest{Name: vol.Name, Size: vol.Size, Type: vol.Type}
		}
	}

	instance, err := m.dcService.CreateInstance(ctx, &input)
	if err != nil {
		return "", hostname, fmt.Errorf("create instance failed: %w", err)
	}

	klog.V(4).Infof("Created instance %s (id=%s) for ASG %s", hostname, instance.ID, asg.Name)
	return instance.ID, hostname, nil
}

func (m *autoScalingGroups) createOrGetStartupScript(ctx context.Context, asg *Asg, nodeConfig *nodeConfig, providerID string) (string, error) {
	scriptName := fmt.Sprintf("as-%s", asg.Name)
	decodedScript, err := base64.StdEncoding.DecodeString(nodeConfig.StartupScript)
	if err != nil {
		return "", fmt.Errorf("failed to decode startup script: %v", err)
	}

	startupScriptEnv := make(map[string]string, len(nodeConfig.StartupScriptEnv)+2)
	for k, v := range nodeConfig.StartupScriptEnv {
		startupScriptEnv[strings.ToUpper(k)] = v
	}
	startupScriptEnv["PROVIDER_ID"] = providerID
	labels := convertConfigLabelsToK8sLabels(nodeConfig.Labels, asg)
	startupScriptEnv["LABELS"] = labels

	klog.V(4).Infof("Patching startup script with PROVIDER_ID=%s, LABELS=%s", providerID, labels)

	patchedScript := injectEnvVarsIntoScript(decodedScript, startupScriptEnv)

	script, err := m.dcService.CreateStartScript(ctx, scriptName, string(patchedScript))
	if err != nil {
		klog.Errorf("CreateStartScript API call failed: %v", err)
		return "", fmt.Errorf("failed to create startup script: %v", err)
	}

	klog.V(4).Infof("Created startup script %s (id=%s)", scriptName, script.ID)
	return script.ID, nil
}

// removeInstanceFromCache requires cacheMutex held.
func (m *autoScalingGroups) removeInstanceFromCache(hostname string, asg *Asg) {
	ref, _ := m.findCachedRefByHostname(hostname)
	if ref == nil {
		return
	}
	delete(m.instanceToAsg, *ref)
	delete(m.instanceIDs, *ref)
	refs := m.asgToInstances[asg.AsgRef]
	for i, r := range refs {
		if r.Hostname == hostname {
			m.asgToInstances[asg.AsgRef] = append(refs[:i], refs[i+1:]...)
			break
		}
	}
	asg.curSize--
}

func (m *autoScalingGroups) InstanceRefsForAsg(ref AsgRef) ([]InstanceRef, error) {
	m.cacheMutex.RLock()
	defer m.cacheMutex.RUnlock()
	return m.asgToInstances[ref], nil
}

func (m *autoScalingGroups) InstancesForAsg(ref AsgRef) ([]verda.Instance, error) {
	ctx := context.Background()

	m.cacheMutex.RLock()
	refs := append([]InstanceRef(nil), m.asgToInstances[ref]...)
	m.cacheMutex.RUnlock()

	allInstances, err := m.dcService.ListInstancesCached(ctx)
	if err != nil {
		return nil, err
	}

	apiByHostname := make(map[string]verda.Instance, len(allInstances))
	for _, inst := range allInstances {
		apiByHostname[inst.Hostname] = inst
	}

	instances := make([]verda.Instance, 0, len(refs))
	for _, r := range refs {
		if inst, ok := apiByHostname[r.Hostname]; ok {
			instances = append(instances, inst)
		} else {
			// Not in API yet; return placeholder
			instances = append(instances, verda.Instance{
				ID:       r.ProviderID,
				Hostname: r.Hostname,
				Status:   verda.StatusOrdered,
			})
		}
	}
	return instances, nil
}

func (m *autoScalingGroups) DeleteAsg(ref AsgRef) error {
	ctx := context.Background()

	m.cacheMutex.RLock()
	instanceRefs := append([]InstanceRef(nil), m.asgToInstances[ref]...)
	asg := m.registeredAsgs[ref]
	// Snapshot instance IDs for deletion
	idsToDelete := make(map[string]string, len(instanceRefs)) // hostname → instance ID
	for _, insRef := range instanceRefs {
		if id, ok := m.instanceIDs[insRef]; ok {
			idsToDelete[insRef.Hostname] = id
		}
	}
	m.cacheMutex.RUnlock()

	var wg sync.WaitGroup
	errsCh := make(chan error, len(instanceRefs))
	for _, insRef := range instanceRefs {
		instanceID, ok := idsToDelete[insRef.Hostname]
		if !ok {
			klog.Warningf("DeleteAsg: no cached instance ID for %s, skipping", insRef.Hostname)
			continue
		}
		wg.Add(1)
		go func(id, hostname string) {
			defer wg.Done()
			if err := m.dcService.PerformInstanceAction(ctx, id, verda.ActionDelete); err != nil {
				errsCh <- fmt.Errorf("delete %s failed: %w", hostname, err)
			}
		}(instanceID, insRef.Hostname)
	}
	wg.Wait()
	close(errsCh)

	var errs []error
	for err := range errsCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to delete all instances: %w", errors.Join(errs...))
	}

	m.cacheMutex.Lock()
	for _, insRef := range instanceRefs {
		delete(m.instanceToAsg, insRef)
		delete(m.instanceIDs, insRef)
	}
	delete(m.registeredAsgs, ref)
	delete(m.asgToInstances, ref)
	delete(m.asgNodeGroupSpecs, ref)
	if asg != nil {
		asg.curSize = 0
	}
	m.cacheMutex.Unlock()
	return nil
}

func (m *autoScalingGroups) DeleteInstance(ref InstanceRef) error {
	ctx := context.Background()

	m.cacheMutex.RLock()
	asg, found := m.instanceToAsg[ref]
	if !found {
		cachedRef, cachedAsg := m.findCachedRefByHostname(ref.Hostname)
		if cachedRef != nil {
			ref = *cachedRef
			asg = cachedAsg
			found = true
		}
	}
	instanceID := m.instanceIDs[ref]
	m.cacheMutex.RUnlock()

	if !found {
		return fmt.Errorf("instance %s not found in any ASG", ref.Hostname)
	}
	if instanceID == "" {
		return fmt.Errorf("no cached instance ID for %s", ref.Hostname)
	}
	if m.dcService == nil {
		return fmt.Errorf("dcService is not initialized")
	}

	klog.V(4).Infof("DeleteInstance: deleting %s (id=%s, curSize: %d)",
		ref.Hostname, instanceID, asg.curSize)

	if err := m.dcService.PerformInstanceAction(ctx, instanceID, verda.ActionDelete); err != nil {
		return fmt.Errorf("delete instance %s failed: %w", ref.Hostname, err)
	}

	m.cacheMutex.Lock()
	m.removeInstanceFromCache(ref.Hostname, asg)
	m.cacheMutex.Unlock()

	return nil
}

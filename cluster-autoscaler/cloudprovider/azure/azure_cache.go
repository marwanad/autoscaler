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

package azure

import (
	"sync"

	klog "k8s.io/klog/v2"
)


type AzureCache struct {
	registeredNodeGroups      map[AzureRef]AzureNodeGroup
	instanceToNodeGroup       map[AzureRef]AzureNodeGroup
	notInRegisteredNodeGroups map[AzureRef]struct{}
	nodeGroupTargetSizeCache       map[AzureRef]int64
	mutex                     sync.Mutex
	interrupt                 chan struct{}
}

func NewAzureCache() *AzureCache {
	cache := &AzureCache{
		registeredNodeGroups:      make(map[AzureRef]AzureNodeGroup),
		instanceToNodeGroup:       make(map[AzureRef]AzureNodeGroup),
		notInRegisteredNodeGroups: make(map[AzureRef]struct{}),
		nodeGroupTargetSizeCache: make(map[AzureRef]int64),
		interrupt:                 make(chan struct{}),
	}
	return cache
}

// Register registers a node group if it hasn't been registered or its config has changed
func (az *AzureCache) Register(newNg AzureNodeGroup) bool {
	az.mutex.Lock()
	defer az.mutex.Unlock()

	newNgAzureRef := newNg.AzureRef()
	existing, found := az.registeredNodeGroups[newNgAzureRef]
	if found {
		if existing.MinSize() == newNg.MinSize() && existing.MaxSize() == newNg.MaxSize()  {
			return false
		}
		az.registeredNodeGroups[newNgAzureRef] = newNg
		klog.V(4).Infof("Updating node group %q", newNgAzureRef.String())
		return true
	}

	klog.V(4).Infof("Registering node group %q", newNgAzureRef.String())
	az.registeredNodeGroups[newNgAzureRef] = newNg
	return true
}

// Unregister returns true if the node group has been removed and false otherwise
func (az *AzureCache) Unregister(toUnregister AzureNodeGroup) bool {
	az.mutex.Lock()
	defer az.mutex.Unlock()

	toBeRemovedAzureRef := toUnregister.AzureRef()
	_, found := az.registeredNodeGroups[toBeRemovedAzureRef]
	if found {
		klog.V(1).Infof("Unregistered node group %q", toBeRemovedAzureRef.String())
		delete(az.registeredNodeGroups, toBeRemovedAzureRef)
		az.removeInstancesForNodegroup(toBeRemovedAzureRef)
		return true
	}
	return false
}

func (az *AzureCache) getNodeGroups() []AzureNodeGroup {
	az.mutex.Lock()
	defer az.mutex.Unlock()

	nodeGroups := make([]AzureNodeGroup, 0, len(az.registeredNodeGroups))
	for _, ng := range az.registeredNodeGroups {
		nodeGroups = append(nodeGroups, ng)
	}
	return nodeGroups
}

// GetNodeGroupForInstance returns the node group of the given Instance
func (az *AzureCache) GetNodeGroupForInstance(instance *AzureRef, vmType string) (AzureNodeGroup, error) {
	az.mutex.Lock()
	defer az.mutex.Unlock()

	klog.V(4).Infof("GetNodeGroupForInstance: starts, ref: %s", instance.String())
	resourceID, err := convertResourceGroupNameToLower(instance.Name)
	if err != nil {
		return nil, err
	}
	inst := AzureRef{Name: resourceID}

	if _, found := az.notInRegisteredNodeGroups[inst]; found {
		// We already know we don't own this instance. Return early and avoid
		// additional calls.
		klog.V(4).Infof("GetNodeGroupForInstance: Couldn't find NodeGroup for instance %q", inst)
		return nil, nil
	}

	// Omit virtual machines not managed by vmss and virtual machines with invalid providerId
	if vmType == vmTypeVMSS  || vmType == vmTypeStandard{
		if ok := validProviderIdRe.Match([]byte(inst.Name)); ok {
			klog.V(3).Infof("Instance %q is not managed by VMSS or has an invalid providerId format, adding it to the unowned list", instance.Name)
			az.notInRegisteredNodeGroups[inst] = struct{}{}
			return nil, nil
		}
	}

	// Look up caches for the instance.
	klog.V(6).Infof("GetNodeGroupForInstance: attempting to retrieve instance %v from cache", az.instanceToNodeGroup)
	if ng := az.getNodeGroupFromCacheNoLock(inst.Name); ng != nil {
		klog.V(4).Infof("GetNodeGroupForInstance: instance %q belongs to node group %q in cache", inst.Name, ng.Id())
		return ng, nil
	}
	klog.V(4).Infof("GetNodeGroupForInstance: Couldn't find node group for instance %q", inst)
	return nil, nil
}

func (az *AzureCache) RegenerateInstanceCache() error {
	az.mutex.Lock()
	defer az.mutex.Unlock()

	klog.V(4).Infof("RegenerateInstanceCache: regenerating instance cache for all node groups")

	az.instanceToNodeGroup = make(map[AzureRef]AzureNodeGroup)
	az.notInRegisteredNodeGroups = make(map[AzureRef]struct{})

	for ngRef, _ := range az.registeredNodeGroups {
		err := az.regenerateInstanceCacheForNodeGroupNoLock(ngRef)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetMigTargetSize returns the cached targetSize for a GceRef
func (az *AzureCache) GetNodeGroupTargetSize(ref AzureRef) (int64, bool) {
	az.mutex.Lock()
	defer az.mutex.Unlock()

	size, found := az.nodeGroupTargetSizeCache[ref]
	if found {
		klog.V(6).Infof("Found target size for node group %q in cache", ref.String())
	}
	return size, found
}

func (az *AzureCache) SetNodeGroupTargetSize(ref AzureRef, size int64) {
	az.mutex.Lock()
	defer az.mutex.Unlock()

	az.nodeGroupTargetSizeCache[ref] = size
}

func (az *AzureCache) GetNodeGroups() []AzureNodeGroup {
	az.mutex.Lock()
	defer az.mutex.Unlock()

	ngs := make([]AzureNodeGroup, 0, len(az.registeredNodeGroups))
	for _, ng := range az.registeredNodeGroups {
		ngs = append(ngs, ng)
	}
	return ngs
}

func (az *AzureCache) regenerateInstanceCacheForNodeGroupNoLock(ngRef AzureRef) error {
	klog.V(4).Infof("regenerateInstanceCacheForNodeGroupNoLock: regenerating instance cache for node group: %s", ngRef.String())
	az.removeInstancesForNodegroup(ngRef)

	ng, _ := az.registeredNodeGroups[ngRef]
	instances, err := ng.Nodes()
	if err != nil {
		return err
	}
	klog.V(6).Infof("regenerateInstanceCacheForNodeGroupNoLock: found nodes for node group %v: %+v", ng, instances)
	for _, instance := range instances {
		ref := AzureRef{Name: instance.Id}
		az.instanceToNodeGroup[ref] = ng
	}

	return nil
}

func (az *AzureCache) removeInstancesForNodegroup(toBeRemovedRef AzureRef) {
	for instanceRef, nodeGroup := range az.instanceToNodeGroup {
		if toBeRemovedRef.String() == nodeGroup.Id() {
			delete(az.instanceToNodeGroup, instanceRef)
			delete(az.notInRegisteredNodeGroups, instanceRef)
		}
	}
}

// Retrieves a node group for a given instance if it exists in the cache
func (az *AzureCache) getNodeGroupFromCacheNoLock(providerID string) AzureNodeGroup {
	ng, found := az.instanceToNodeGroup[AzureRef{Name: providerID}]
	if found {
		return ng
	}
	return nil
}

// Cleanup closes the channel to signal the go routine to stop that is handling the cache
func (az *AzureCache) Cleanup() {
	close(az.interrupt)
}

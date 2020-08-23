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

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	klog "k8s.io/klog/v2"
)


type azureCache struct {
	registeredNodeGroups      map[azureRef]cloudprovider.NodeGroup
	instanceToNodeGroup       map[azureRef]cloudprovider.NodeGroup
	notInRegisteredNodeGroups map[azureRef]struct{}
	mutex                     sync.Mutex
	interrupt                 chan struct{}
}

func NewAzureCache() *azureCache {
	cache := &azureCache{
		registeredNodeGroups:      make(map[azureRef]cloudprovider.NodeGroup),
		instanceToNodeGroup:       make(map[azureRef]cloudprovider.NodeGroup),
		notInRegisteredNodeGroups: make(map[azureRef]struct{}),
		interrupt:                 make(chan struct{}),
	}
	return cache
}

// Register registers a node group if it hasn't been registered or its config has changed
func (az *azureCache) Register(newNg cloudprovider.NodeGroup) bool {
	az.mutex.Lock()
	defer az.mutex.Unlock()

	newNgAzureRef := azureRef{newNg.Id()}
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
func (az *azureCache) Unregister(toUnregister cloudprovider.NodeGroup) bool {
	az.mutex.Lock()
	defer az.mutex.Unlock()

	toBeRemovedAzureRef:= azureRef{toUnregister.Id()}
	_, found := az.registeredNodeGroups[toBeRemovedAzureRef]
	if found {
		klog.V(1).Infof("Unregistered node group %q", toBeRemovedAzureRef.String())
		delete(az.registeredNodeGroups, toBeRemovedAzureRef)
		az.removeInstancesForNodegroup(toBeRemovedAzureRef)
		return true
	}
	return false
}

func (az *azureCache) getNodeGroups() []cloudprovider.NodeGroup {
	az.mutex.Lock()
	defer az.mutex.Unlock()

	nodeGroups := make([]cloudprovider.NodeGroup, 0, len(az.registeredNodeGroups))
	for _, ng := range az.registeredNodeGroups {
		nodeGroups = append(nodeGroups, ng)
	}
	return nodeGroups
}

// GetNodeGroupForInstance returns the node group of the given Instance
func (az *azureCache) GetNodeGroupForInstance(instance *azureRef, vmType string) (cloudprovider.NodeGroup, error) {
	az.mutex.Lock()
	defer az.mutex.Unlock()

	klog.V(4).Infof("GetNodeGroupForInstance: starts, ref: %s", instance.String())
	resourceID, err := convertResourceGroupNameToLower(instance.Name)
	if err != nil {
		return nil, err
	}
	inst := azureRef{Name: resourceID}

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

func (az *azureCache) RegenerateInstanceCache() error {
	az.mutex.Lock()
	defer az.mutex.Unlock()

	klog.V(4).Infof("RegenerateInstanceCache: regenerating instance cache for all node groups")

	az.instanceToNodeGroup = make(map[azureRef]cloudprovider.NodeGroup)
	az.notInRegisteredNodeGroups = make(map[azureRef]struct{})

	for ngRef, _ := range az.registeredNodeGroups {
		err := az.regenerateInstanceCacheForNodeGroupNoLock(ngRef)
		if err != nil {
			return err
		}
	}
	return nil
}

func (az *azureCache) regenerateInstanceCacheForNodeGroupNoLock(ngRef azureRef) error {
	klog.V(4).Infof("regenerateInstanceCacheForNodeGroupNoLock: regenerating instance cache for node group: %s", ngRef.String())
	az.removeInstancesForNodegroup(ngRef)

	ng, _ := az.registeredNodeGroups[ngRef]
	instances, err := ng.Nodes()
	if err != nil {
		return err
	}
	klog.V(6).Infof("regenerateInstanceCacheForNodeGroupNoLock: found nodes for node group %v: %+v", ng, instances)
	for _, instance := range instances {
		ref := azureRef{Name: instance.Id}
		az.instanceToNodeGroup[ref] = ng
	}

	return nil
}

func (az *azureCache) removeInstancesForNodegroup(toBeRemovedRef azureRef) {
	for instanceRef, nodeGroup := range az.instanceToNodeGroup {
		if toBeRemovedRef.String() == nodeGroup.Id() {
			delete(az.instanceToNodeGroup, instanceRef)
			delete(az.notInRegisteredNodeGroups, instanceRef)
		}
	}
}

// Retrieves a node group for a given instance if it exists in the cache
func (az *azureCache) getNodeGroupFromCacheNoLock(providerID string) cloudprovider.NodeGroup {
	ng, found := az.instanceToNodeGroup[azureRef{Name: providerID}]
	if found {
		return ng
	}
	return nil
}

// Cleanup closes the channel to signal the go routine to stop that is handling the cache
func (az *azureCache) Cleanup() {
	close(az.interrupt)
}

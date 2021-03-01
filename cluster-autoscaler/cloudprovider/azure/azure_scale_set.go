/*
Copyright 2017 The Kubernetes Authors.

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
	"fmt"
	"github.com/Azure/azure-sdk-for-go/services/containerservice/mgmt/2020-11-01/containerservice"
	"github.com/Azure/go-autorest/autorest/to"
	"hash/fnv"
	"k8s.io/apimachinery/pkg/api/resource"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config/dynamic"
	klog "k8s.io/klog/v2"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"k8s.io/legacy-cloud-providers/azure/retry"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2019-12-01/compute"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
)

var (
	defaultVmssSizeRefreshPeriod      = 1 * time.Minute
	defaultVmssInstancesRefreshPeriod = 5 * time.Minute
	vmssContextTimeout                = 3 * time.Minute
	vmssSizeMutex                     sync.Mutex
)

var scaleSetStatusCache struct {
	lastRefresh time.Time
	mutex       sync.Mutex
	scaleSets   map[string]compute.VirtualMachineScaleSet
}

func init() {
	// In go-autorest SDK https://github.com/Azure/go-autorest/blob/master/autorest/sender.go#L242,
	// if ARM returns http.StatusTooManyRequests, the sender doesn't increase the retry attempt count,
	// hence the Azure clients will keep retrying forever until it get a status code other than 429.
	// So we explicitly removes http.StatusTooManyRequests from autorest.StatusCodesForRetry.
	// Refer https://github.com/Azure/go-autorest/issues/398.
	statusCodesForRetry := make([]int, 0)
	for _, code := range autorest.StatusCodesForRetry {
		if code != http.StatusTooManyRequests {
			statusCodesForRetry = append(statusCodesForRetry, code)
		}
	}
	autorest.StatusCodesForRetry = statusCodesForRetry
}

type autoprovisioningSpec struct {
	machineType    string
	labels         map[string]string
	taints         []apiv1.Taint
	zonal 		   bool
	zone		   string
	extraResources map[string]resource.Quantity
}

// ScaleSet implements NodeGroup interface.
type ScaleSet struct {
	azureRef
	manager *AzureManager

	minSize int
	maxSize int

	sizeMutex            sync.Mutex
	curSize              int64
	lastSizeRefresh      time.Time
	sizeRefreshPeriod    time.Duration
	exists               bool
	autoprovisioned      bool
	zone 				 string
	vmssName string
	autoProvisioningSpec *autoprovisioningSpec

	instancesRefreshPeriod time.Duration
	instancesRefreshJitter int

	instanceMutex       sync.Mutex
	instanceCache       []cloudprovider.Instance
	lastInstanceRefresh time.Time
}

// NewScaleSet creates a new NewScaleSet.
func NewScaleSet(spec *dynamic.NodeGroupSpec, az *AzureManager, curSize int64, exists bool, autoprovisioned bool) (*ScaleSet, error) {
	scaleSet := &ScaleSet{
		azureRef: azureRef{
			Name: spec.Name,
		},
		minSize: spec.MinSize,
		maxSize: spec.MaxSize,
		manager: az,
		curSize: curSize,
		exists: exists,
		autoprovisioned: autoprovisioned,

		instancesRefreshJitter: az.config.VmssVmsCacheJitter,
	}

	if az.config.VmssCacheTTL != 0 {
		scaleSet.sizeRefreshPeriod = time.Duration(az.config.VmssCacheTTL) * time.Second
	} else {
		scaleSet.sizeRefreshPeriod = defaultVmssSizeRefreshPeriod
	}

	if az.config.VmssVmsCacheTTL != 0 {
		scaleSet.instancesRefreshPeriod = time.Duration(az.config.VmssVmsCacheTTL) * time.Second
	} else {
		scaleSet.instancesRefreshPeriod = defaultVmssInstancesRefreshPeriod
	}

	return scaleSet, nil
}

// MinSize returns minimum size of the node group.
func (scaleSet *ScaleSet) MinSize() int {
	return scaleSet.minSize
}

// Exist checks if the node group really exists on the cloud provider side. Allows to tell the
// theoretical node group from the real one.
func (scaleSet *ScaleSet) Exist() bool {
	return scaleSet.exists
}

// Create creates the node group on the cloud provider side.
func (scaleSet *ScaleSet) Create(targetSize int) (cloudprovider.NodeGroup, error) {
	if !scaleSet.Exist() && scaleSet.Autoprovisioned() {
		return scaleSet.createNodepool(targetSize)
	} else {
		klog.Infof("Not attempting to create nodepool (%s/%s) because it already exists", scaleSet.manager.config.ClusterName, scaleSet.Name)
		return scaleSet, cloudprovider.ErrAlreadyExist
	}
}

// Delete deletes the node group on the cloud provider side.
// This will be executed only for autoprovisioned node groups, once their size drops to 0.
func (scaleSet *ScaleSet) Delete() error {
	if scaleSet.Autoprovisioned() {
		updateCtx, updateCancel := getContextWithCancel()
		defer updateCancel()
		rgName := scaleSet.manager.config.ResourceGroup
		clusterName := scaleSet.manager.config.ClusterName

		aksClient := scaleSet.manager.azClient.agentPoolsClient
		err := aksClient.Delete(updateCtx, rgName, clusterName, scaleSet.azureRef.Name)
		if err != nil {
			klog.Errorf("Failed to delete AKS nodepool (%q): %v", clusterName, err.Error())
			return err.Error()
		}

		scaleSet.exists = false
		return nil
	}

	return cloudprovider.ErrNotImplemented}

// Autoprovisioned returns true if the node group is autoprovisioned.
func (scaleSet *ScaleSet) Autoprovisioned() bool {
	return scaleSet.autoprovisioned
}


func (scaleSet *ScaleSet) createNodepool(targetSize int) (cloudprovider.NodeGroup, error) {
	ctx, cancel := getContextWithCancel()
	defer cancel()

	rgName := scaleSet.manager.config.ResourceGroup

	clusterName := scaleSet.manager.config.ClusterName
	managedCluster, rerr := scaleSet.manager.azClient.managedKubernetesServicesClient.Get(ctx, rgName, clusterName)
	if rerr != nil {
		klog.Errorf("Failed to get AKS cluster (name:%s): %s", clusterName, rerr.Error())
		return nil, rerr.Error()
	}

	labels := map[string]*string{}
	taints := []string{}

	for k, v := range scaleSet.autoProvisioningSpec.labels {
		labels[k] = &v
	}

	for _, v := range scaleSet.autoProvisioningSpec.taints {
		taints = append(taints, v.String())
	}

	// TODO(ace): HACK DUE TO KNOWLEDGE OF AKS IMPLEMENTATION
	// hasn't changed in years though.
	// uniqueNameSuffixSize := 8
	// h := fnv.New64a()
	// h.Write([]byte(*managedCluster.Fqdn))
	// r := rand.New(rand.NewSource(int64(h.Sum64())))
	// clusterID := fmt.Sprintf("%08d\n", r.Uint32())[:uniqueNameSuffixSize]
	// _ = clusterID
	// vnetSubnetID := (*managedCluster.AgentPoolProfiles)[0].VnetSubnetID
	pool := containerservice.AgentPool{
		ManagedClusterAgentPoolProfileProperties: &containerservice.ManagedClusterAgentPoolProfileProperties{
			VMSize:              containerservice.VMSizeTypes(scaleSet.autoProvisioningSpec.machineType),
			OsType:              containerservice.Linux,
			// create with zero, then issue scale up
			Count:               to.Int32Ptr(int32(0)),
			Type:                containerservice.VirtualMachineScaleSets,
			OrchestratorVersion: managedCluster.ManagedClusterProperties.KubernetesVersion,
			Tags: map[string]*string{
				"autoprovisioned": to.StringPtr("autonoms"),
			},
			NodeLabels: labels,
			NodeTaints: &taints,
		},
	}

	updateCtx, updateCancel := getContextWithCancel()
	defer updateCancel()
	aksClient := scaleSet.manager.azClient.agentPoolsClient

	// TODO (marwan) come up with autogenerated way to
	// Use an acceptable name for the agentpool
	// because the auto-generated one for the node templates isn't valid
	uniqueNameSuffixSize := 8
	h := fnv.New64a()
	h.Write([]byte(time.Now().String()))
	r := rand.New(rand.NewSource(int64(h.Sum64())))
	nodePoolName := fmt.Sprintf("a%08d\n", r.Uint32())[:uniqueNameSuffixSize]

	numZones := 1
	if scaleSet.autoProvisioningSpec.zonal {
		numZones = 3
	}
	for i := 1; i <= numZones; i++ {
		poolName := fmt.Sprintf("%sz%d", nodePoolName, i)
		pool.AvailabilityZones = to.StringSlicePtr([]string{fmt.Sprintf("%d", i)})
		klog.Infof("Choosing name: %s for autoprovisioned nodeGroup: %s", poolName, scaleSet.Name)

		rerr = aksClient.CreateOrUpdate(updateCtx, rgName, clusterName, poolName, pool, "")
		if rerr != nil {
			klog.Errorf("Failed to create AKS nodepool (%q) - %d for cluster %q: %v", poolName, int32(targetSize), clusterName, rerr.Error())
			return nil, rerr.Error()
		}
		klog.Infof("Successfully created nodepool %s", poolName)
	}

	//go scaleSet.waitForCreateOrUpdateAp(future)
	//// Proactively set it to exist, so that we don't hammer more calls
	//// eventually the 1 minute refresh will take over and the
	///  VMSS will be created
	//scaleSet.exists = true

	// After the scale succeeds, register the VMSS as a CA nodegroup
	err := scaleSet.manager.fetchAutoprovisionedGroups()
	if err != nil {
		klog.Errorf("Error fetching auto provisioned node groups %s", err)
		return nil, err
	}

	// Ensure the scaleSet is registered and return it to the caller
	// The caller can then choose to scale it up (seems to be the expectation anyways)
	for _, ap := range scaleSet.manager.getAsgs() {
		poolNameToReturn := nodePoolName
		if scaleSet.autoProvisioningSpec.zonal {
			// Return the first zone for the main group
			poolNameToReturn = fmt.Sprintf("%sz1", poolNameToReturn)
		}
		if ap.Id() == poolNameToReturn {
				return ap, nil
		}
	}
	return nil, fmt.Errorf("agentpool %s for autoprovisioned template %s not found", nodePoolName, scaleSet.Name)
}


func (scaleSet *ScaleSet) waitForCreateOrUpdateAp(future *azure.Future) {
	var err error
	aksClient := scaleSet.manager.azClient.agentPoolsClient

	defer func() {
		if err != nil {
		}
	}()

	ctx, cancel := getContextWithCancel()
	defer cancel()

	klog.V(3).Infof("Calling aksClient.WaitForCreateOrUpdateResult(%s)", scaleSet.azureRef.Name)
	httpResponse, err := aksClient.WaitForCreateOrUpdateResult(ctx, future, scaleSet.manager.config.ResourceGroup)

	isSuccess, err := isSuccessHTTPResponse(httpResponse, err)
	if isSuccess {
		klog.V(3).Infof("aksClient.WaitForCreateOrUpdateResult(%s) success", scaleSet.azureRef.Name)
	} else {
		klog.Errorf("Failed to create or update agentpool", scaleSet.Name, err)
	}
}

// MaxSize returns maximum size of the node group.
func (scaleSet *ScaleSet) MaxSize() int {
	return scaleSet.maxSize
}

func (scaleSet *ScaleSet) getVMSSInfo() (compute.VirtualMachineScaleSet, *retry.Error) {
	scaleSetStatusCache.mutex.Lock()
	defer scaleSetStatusCache.mutex.Unlock()

	if scaleSetStatusCache.lastRefresh.Add(scaleSet.sizeRefreshPeriod).After(time.Now()) {
		if status, exists := scaleSetStatusCache.scaleSets[scaleSet.Name]; exists {
			return status, nil
		}
	}

	var allVMSS []compute.VirtualMachineScaleSet
	var rerr *retry.Error

	allVMSS, rerr = scaleSet.getAllVMSSInfo()
	if rerr != nil {
		return compute.VirtualMachineScaleSet{}, rerr
	}

	var newStatus = make(map[string]compute.VirtualMachineScaleSet)
	for _, vmss := range allVMSS {
		// If autoprovisioned, the scaleSet name is a substring of the vmss name
		if val, ok := vmss.Tags["autoprovisioned"]; ok {
			if val != nil && strings.EqualFold(*val, "autonoms") {
				if poolName, exist := vmss.Tags["poolName"]; exist {
					klog.V(1).Infof("Scale set :%s is autoprovisioned, registering it under the AKS name: %s", *vmss.Name, *poolName)
					newStatus[*poolName] = vmss
				}
			}
		} else {
			newStatus[*vmss.Name] = vmss
		}
	}

	scaleSetStatusCache.lastRefresh = time.Now()
	scaleSetStatusCache.scaleSets = newStatus

	if _, exists := scaleSetStatusCache.scaleSets[scaleSet.Name]; !exists {
		return compute.VirtualMachineScaleSet{}, &retry.Error{RawError: fmt.Errorf("could not find vmss: %s", scaleSet.Name)}
	}

	return scaleSetStatusCache.scaleSets[scaleSet.Name], nil
}

func (scaleSet *ScaleSet) getAllVMSSInfo() ([]compute.VirtualMachineScaleSet, *retry.Error) {
	ctx, cancel := getContextWithTimeout(vmssContextTimeout)
	defer cancel()

	resourceGroup := scaleSet.manager.config.NodeResourceGroup
	setInfo, rerr := scaleSet.manager.azClient.virtualMachineScaleSetsClient.List(ctx, resourceGroup)
	if rerr != nil {
		return []compute.VirtualMachineScaleSet{}, rerr
	}

	return setInfo, nil
}

func (scaleSet *ScaleSet) getCurSize() (int64, error) {
	scaleSet.sizeMutex.Lock()
	defer scaleSet.sizeMutex.Unlock()

	if scaleSet.lastSizeRefresh.Add(scaleSet.sizeRefreshPeriod).After(time.Now()) {
		return scaleSet.curSize, nil
	}

	klog.V(5).Infof("Get scale set size for %q", scaleSet.Name)
	set, rerr := scaleSet.getVMSSInfo()
	if rerr != nil {
		if isAzureRequestsThrottled(rerr) {
			// Log a warning and update the size refresh time so that it would retry after next sizeRefreshPeriod.
			klog.Warningf("getVMSSInfo() is throttled with message %v, would return the cached vmss size", rerr)
			scaleSet.lastSizeRefresh = time.Now()
			return scaleSet.curSize, nil
		}
		return -1, rerr.Error()
	}

	vmssSizeMutex.Lock()
	curSize := *set.Sku.Capacity
	vmssSizeMutex.Unlock()

	klog.V(5).Infof("Getting scale set (%q) capacity: %d\n", scaleSet.Name, curSize)

	if scaleSet.curSize != curSize {
		// Invalidate the instance cache if the capacity has changed.
		scaleSet.invalidateInstanceCache()
	}

	scaleSet.curSize = curSize
	scaleSet.lastSizeRefresh = time.Now()
	return scaleSet.curSize, nil
}

// GetScaleSetSize gets Scale Set size.
func (scaleSet *ScaleSet) GetScaleSetSize() (int64, error) {
	return scaleSet.getCurSize()
}

func (scaleSet *ScaleSet) waitForDeleteInstances(future *azure.Future, requiredIds *compute.VirtualMachineScaleSetVMInstanceRequiredIDs) {
	ctx, cancel := getContextWithCancel()
	defer cancel()

	klog.V(3).Infof("Calling virtualMachineScaleSetsClient.WaitForAsyncOperationResult - DeleteInstances(%v)", requiredIds.InstanceIds)
	httpResponse, err := scaleSet.manager.azClient.virtualMachineScaleSetsClient.WaitForAsyncOperationResult(ctx, future)
	isSuccess, err := isSuccessHTTPResponse(httpResponse, err)
	if isSuccess {
		klog.V(3).Infof("virtualMachineScaleSetsClient.WaitForAsyncOperationResult - DeleteInstances(%v) success", requiredIds.InstanceIds)
		return
	}
	klog.Errorf("virtualMachineScaleSetsClient.WaitForAsyncOperationResult - DeleteInstances for instances %v failed with error: %v", requiredIds.InstanceIds, err)
}

// updateVMSSCapacity invokes virtualMachineScaleSetsClient to update the capacity for VMSS.
func (scaleSet *ScaleSet) updateVMSSCapacity(future *azure.Future) {
	var err error

	defer func() {
		if err != nil {
			klog.Errorf("Failed to update the capacity for vmss %s with error %v, invalidate the cache so as to get the real size from API", scaleSet.Name, err)
			// Invalidate the VMSS size cache in order to fetch the size from the API.
			scaleSet.invalidateStatusCacheWithLock()
		}
	}()

	ctx, cancel := getContextWithCancel()
	defer cancel()

	klog.V(3).Infof("Calling virtualMachineScaleSetsClient.WaitForAsyncOperationResult - updateVMSSCapacity(%s)", scaleSet.Name)
	httpResponse, err := scaleSet.manager.azClient.virtualMachineScaleSetsClient.WaitForAsyncOperationResult(ctx, future)

	isSuccess, err := isSuccessHTTPResponse(httpResponse, err)
	if isSuccess {
		klog.V(3).Infof("virtualMachineScaleSetsClient.WaitForAsyncOperationResult - updateVMSSCapacity(%s) success", scaleSet.Name)
		scaleSet.invalidateInstanceCache()
		return
	}

	klog.Errorf("virtualMachineScaleSetsClient.WaitForAsyncOperationResult - updateVMSSCapacity for scale set %q failed: %v", scaleSet.Name, err)
}

// SetScaleSetSize sets ScaleSet size.
func (scaleSet *ScaleSet) SetScaleSetSize(size int64) error {
	scaleSet.sizeMutex.Lock()
	defer scaleSet.sizeMutex.Unlock()

	vmssInfo, rerr := scaleSet.getVMSSInfo()
	if rerr != nil {
		klog.Errorf("Failed to get information for VMSS (%q): %v", scaleSet.Name, rerr)
		return rerr.Error()
	}

	// Update the new capacity to cache.
	vmssSizeMutex.Lock()
	vmssInfo.Sku.Capacity = &size
	vmssSizeMutex.Unlock()

	// Compose a new VMSS for updating.
	op := compute.VirtualMachineScaleSet{
		Name:     vmssInfo.Name,
		Sku:      vmssInfo.Sku,
		Location: vmssInfo.Location,
	}
	ctx, cancel := getContextWithTimeout(vmssContextTimeout)
	defer cancel()
	klog.V(3).Infof("Waiting for virtualMachineScaleSetsClient.CreateOrUpdateAsync(%s)", scaleSet.Name)
	future, rerr := scaleSet.manager.azClient.virtualMachineScaleSetsClient.CreateOrUpdateAsync(ctx, scaleSet.manager.config.NodeResourceGroup, scaleSet.vmssName, op)
	if rerr != nil {
		klog.Errorf("virtualMachineScaleSetsClient.CreateOrUpdate for scale set %q failed: %v", scaleSet.Name, rerr)
		return rerr.Error()
	}

	// Proactively set the VMSS size so autoscaler makes better decisions.
	scaleSet.curSize = size
	scaleSet.lastSizeRefresh = time.Now()

	go scaleSet.updateVMSSCapacity(future)

	return nil
}

// TargetSize returns the current TARGET size of the node group. It is possible that the
// number is different from the number of nodes registered in Kubernetes.
func (scaleSet *ScaleSet) TargetSize() (int, error) {
	if scaleSet.Exist() {
		size, err := scaleSet.GetScaleSetSize()
		return int(size), err
	}
	klog.V(1).Infof("ScaleSet: %s doesn't exist yet, returning the supposed node count %d", scaleSet.Name, scaleSet.curSize)
	return int(scaleSet.curSize), nil
}

// IncreaseSize increases Scale Set size
func (scaleSet *ScaleSet) IncreaseSize(delta int) error {
	if delta <= 0 {
		return fmt.Errorf("size increase must be positive")
	}
	if scaleSet.Exist() {
		if scaleSet.vmssName == "" {
			return fmt.Errorf("ScaleSet: %q is still under creation. Backing-off", scaleSet.Name)
		}
		size, err := scaleSet.GetScaleSetSize()
		if err != nil {
			return err
		}

		if size == -1 {
			return fmt.Errorf("the scale set %s is under initialization, skipping IncreaseSize", scaleSet.Name)
		}

		if int(size)+delta > scaleSet.MaxSize() {
			return fmt.Errorf("size increase too large - desired:%d max:%d", int(size)+delta, scaleSet.MaxSize())
		}

		return scaleSet.SetScaleSetSize(size + int64(delta))
	}
	return fmt.Errorf("scale set (%q) doesn't exist. Skipping increase size", scaleSet.Name)
}

// GetScaleSetVms returns list of nodes for the given scale set.
func (scaleSet *ScaleSet) GetScaleSetVms() ([]compute.VirtualMachineScaleSetVM, *retry.Error) {
	klog.V(4).Infof("GetScaleSetVms: starts")
	ctx, cancel := getContextWithTimeout(vmssContextTimeout)
	defer cancel()

	resourceGroup := scaleSet.manager.config.NodeResourceGroup
	vmList, rerr := scaleSet.manager.azClient.virtualMachineScaleSetVMsClient.List(ctx, resourceGroup, scaleSet.vmssName, "")
	klog.V(4).Infof("GetScaleSetVms: scaleSet.Name: %s, vmList: %v", scaleSet.Name, vmList)
	if rerr != nil {
		klog.Errorf("VirtualMachineScaleSetVMsClient.List failed for %s: %v", scaleSet.Name, rerr)
		return nil, rerr
	}

	return vmList, nil
}

// DecreaseTargetSize decreases the target size of the node group. This function
// doesn't permit to delete any existing node and can be used only to reduce the
// request for new nodes that have not been yet fulfilled. Delta should be negative.
// It is assumed that cloud provider will not delete the existing nodes if the size
// when there is an option to just decrease the target.
func (scaleSet *ScaleSet) DecreaseTargetSize(delta int) error {
	// VMSS size would be changed automatically after the Node deletion, hence this operation is not required.
	// To prevent some unreproducible bugs, an extra refresh of cache is needed.
	scaleSet.invalidateInstanceCache()
	_, err := scaleSet.GetScaleSetSize()
	if err != nil {
		klog.Warningf("DecreaseTargetSize: failed with error: %v", err)
	}
	return err
}

// Belongs returns true if the given node belongs to the NodeGroup.
func (scaleSet *ScaleSet) Belongs(node *apiv1.Node) (bool, error) {
	klog.V(6).Infof("Check if node belongs to this scale set: scaleset:%v, node:%v\n", scaleSet, node)

	ref := &azureRef{
		Name: node.Spec.ProviderID,
	}

	targetAsg, err := scaleSet.manager.GetAsgForInstance(ref)
	if err != nil {
		return false, err
	}
	if targetAsg == nil {
		return false, fmt.Errorf("%s doesn't belong to a known scale set", node.Name)
	}
	if !strings.EqualFold(targetAsg.Id(), scaleSet.Id()) {
		return false, nil
	}
	return true, nil
}

// DeleteInstances deletes the given instances. All instances must be controlled by the same ASG.
func (scaleSet *ScaleSet) DeleteInstances(instances []*azureRef) error {
	if len(instances) == 0 {
		return nil
	}

	klog.V(3).Infof("Deleting vmss instances %v", instances)

	//commonAsg, err := scaleSet.manager.GetAsgForInstance(instances[0])
	//if err != nil {
	//	return err
	//}

	instanceIDs := []string{}
	for _, instance := range instances {
		//asg, err := scaleSet.manager.GetAsgForInstance(instance)
		//if err != nil {
		//	return err
		//}
		//
		//if !strings.EqualFold(asg.Id(), commonAsg.Id()) {
		//	return fmt.Errorf("cannot delete instance (%s) which don't belong to the same Scale Set (%q)", instance.Name, commonAsg)
		//}

		if cpi, found := scaleSet.getInstanceByProviderID(instance.Name); found && cpi.Status != nil && cpi.Status.State == cloudprovider.InstanceDeleting {
			klog.V(3).Infof("Skipping deleting instance %s as its current state is deleting", instance.Name)
			continue
		}

		instanceID, err := getLastSegment(instance.Name)
		if err != nil {
			klog.Errorf("getLastSegment failed with error: %v", err)
			return err
		}

		instanceIDs = append(instanceIDs, instanceID)
	}

	// nothing to delete
	if len(instanceIDs) == 0 {
		klog.V(3).Infof("No new instances eligible for deletion, skipping")
		return nil
	}

	requiredIds := &compute.VirtualMachineScaleSetVMInstanceRequiredIDs{
		InstanceIds: &instanceIDs,
	}

	ctx, cancel := getContextWithTimeout(vmssContextTimeout)
	defer cancel()
	resourceGroup := scaleSet.manager.config.NodeResourceGroup

	scaleSet.instanceMutex.Lock()
	klog.V(3).Infof("Calling virtualMachineScaleSetsClient.DeleteInstancesAsync(%v)", requiredIds.InstanceIds)
	future, rerr := scaleSet.manager.azClient.virtualMachineScaleSetsClient.DeleteInstancesAsync(ctx, resourceGroup, scaleSet.vmssName, *requiredIds)
	scaleSet.instanceMutex.Unlock()
	if rerr != nil {
		klog.Errorf("virtualMachineScaleSetsClient.DeleteInstancesAsync for instances %v failed: %v", requiredIds.InstanceIds, rerr)
		return rerr.Error()
	}

	// Proactively decrement scale set size so that we don't
	// go below minimum node count if cache data is stale
	scaleSet.sizeMutex.Lock()
	scaleSet.curSize -= int64(len(instanceIDs))
	scaleSet.sizeMutex.Unlock()

	go scaleSet.waitForDeleteInstances(future, requiredIds)
	return nil
}

// DeleteNodes deletes the nodes from the group.
func (scaleSet *ScaleSet) DeleteNodes(nodes []*apiv1.Node) error {
	klog.V(8).Infof("Delete nodes requested: %q\n", nodes)
	size, err := scaleSet.GetScaleSetSize()
	if err != nil {
		return err
	}

	if int(size) <= scaleSet.MinSize() {
		return fmt.Errorf("min size reached, nodes will not be deleted")
	}

	refs := make([]*azureRef, 0, len(nodes))
	for _, node := range nodes {
		belongs, err := scaleSet.Belongs(node)
		if err != nil {
			return err
		}

		if belongs != true {
			return fmt.Errorf("%s belongs to a different asg than %s", node.Name, scaleSet.Id())
		}

		ref := &azureRef{
			Name: node.Spec.ProviderID,
		}
		refs = append(refs, ref)
	}

	return scaleSet.DeleteInstances(refs)
}

// Id returns ScaleSet id.
func (scaleSet *ScaleSet) Id() string {
	return scaleSet.Name
}

// Debug returns a debug string for the Scale Set.
func (scaleSet *ScaleSet) Debug() string {
	return fmt.Sprintf("%s (%d:%d)", scaleSet.Id(), scaleSet.MinSize(), scaleSet.MaxSize())
}

// TemplateNodeInfo returns a node template for this scale set.
func (scaleSet *ScaleSet) TemplateNodeInfo() (*schedulerframework.NodeInfo, error) {
	var node *apiv1.Node
	var err error
	if scaleSet.Exist() {
		template, rerr := scaleSet.getVMSSInfo()
		if rerr != nil {
			return nil, rerr.Error()
		}

		node, err = buildNodeFromTemplate(scaleSet.Name, template)
		if err != nil {
			return nil, err
		}

		// If it doesn't exist and it's autoprovisioned, predict what it's gonna look like
	} else if scaleSet.Autoprovisioned() {
		var err error
		node, err = buildNodeFromAutoprovisioningSpec(scaleSet, scaleSet.manager.config.Location)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("unable to get node info for %s/%s", scaleSet.manager.config.ClusterName, scaleSet.Name)
	}


	nodeInfo := schedulerframework.NewNodeInfo(cloudprovider.BuildKubeProxy(scaleSet.Name))
	nodeInfo.SetNode(node)
	return nodeInfo, nil
}

// Nodes returns a list of all nodes that belong to this node group.
func (scaleSet *ScaleSet) Nodes() ([]cloudprovider.Instance, error) {
	klog.V(4).Infof("Nodes: starts, scaleSet.Name: %s", scaleSet.Name)
	curSize, err := scaleSet.getCurSize()
	if err != nil {
		klog.Errorf("Failed to get current size for vmss %q: %v", scaleSet.Name, err)
		return nil, err
	}

	scaleSet.instanceMutex.Lock()
	defer scaleSet.instanceMutex.Unlock()

	if int64(len(scaleSet.instanceCache)) == curSize &&
		scaleSet.lastInstanceRefresh.Add(scaleSet.instancesRefreshPeriod).After(time.Now()) {
		klog.V(4).Infof("Nodes: returns with curSize %d", curSize)
		return scaleSet.instanceCache, nil
	}

	klog.V(4).Infof("Nodes: starts to get VMSS VMs")

	lastRefresh := time.Now()
	if scaleSet.lastInstanceRefresh.IsZero() && scaleSet.instancesRefreshJitter > 0 {
		// new VMSS: spread future refreshs
		splay := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(scaleSet.instancesRefreshJitter + 1)
		lastRefresh = time.Now().Add(-time.Second * time.Duration(splay))
	}

	vms, rerr := scaleSet.GetScaleSetVms()
	if rerr != nil {
		if isAzureRequestsThrottled(rerr) {
			// Log a warning and update the instance refresh time so that it would retry after next scaleSet.instanceRefreshPeriod.
			klog.Warningf("GetScaleSetVms() is throttled with message %v, would return the cached instances", rerr)
			scaleSet.lastInstanceRefresh = lastRefresh
			return scaleSet.instanceCache, nil
		}
		return nil, rerr.Error()
	}

	scaleSet.instanceCache = buildInstanceCache(vms)
	scaleSet.lastInstanceRefresh = lastRefresh
	klog.V(4).Infof("Nodes: returns")
	return scaleSet.instanceCache, nil
}

// Note that the GetScaleSetVms() results is not used directly because for the List endpoint,
// their resource ID format is not consistent with Get endpoint
func buildInstanceCache(vms []compute.VirtualMachineScaleSetVM) []cloudprovider.Instance {
	instances := []cloudprovider.Instance{}

	for _, vm := range vms {
		// The resource ID is empty string, which indicates the instance may be in deleting state.
		if len(*vm.ID) == 0 {
			continue
		}

		resourceID, err := convertResourceGroupNameToLower(*vm.ID)
		if err != nil {
			// This shouldn't happen. Log a waring message for tracking.
			klog.Warningf("buildInstanceCache.convertResourceGroupNameToLower failed with error: %v", err)
			continue
		}

		instances = append(instances, cloudprovider.Instance{
			Id:     "azure://" + resourceID,
			Status: instanceStatusFromVM(vm),
		})
	}

	return instances
}

func (scaleSet *ScaleSet) getInstanceByProviderID(providerID string) (cloudprovider.Instance, bool) {
	scaleSet.instanceMutex.Lock()
	defer scaleSet.instanceMutex.Unlock()
	for _, instance := range scaleSet.instanceCache {
		if instance.Id == providerID {
			return instance, true
		}
	}
	return cloudprovider.Instance{}, false
}

// instanceStatusFromVM converts the VM provisioning state to cloudprovider.InstanceStatus
func instanceStatusFromVM(vm compute.VirtualMachineScaleSetVM) *cloudprovider.InstanceStatus {
	if vm.ProvisioningState == nil {
		return nil
	}

	status := &cloudprovider.InstanceStatus{}
	switch *vm.ProvisioningState {
	case string(compute.ProvisioningStateDeleting):
		status.State = cloudprovider.InstanceDeleting
	case string(compute.ProvisioningStateCreating):
		status.State = cloudprovider.InstanceCreating
	default:
		status.State = cloudprovider.InstanceRunning
	}

	return status
}

func (scaleSet *ScaleSet) invalidateInstanceCache() {
	scaleSet.instanceMutex.Lock()
	// Set the instanceCache as outdated.
	scaleSet.lastInstanceRefresh = time.Now().Add(-1 * scaleSet.instancesRefreshPeriod)
	scaleSet.instanceMutex.Unlock()
}

func (scaleSet *ScaleSet) invalidateStatusCacheWithLock() {
	scaleSet.sizeMutex.Lock()
	scaleSet.lastSizeRefresh = time.Now().Add(-1 * scaleSet.sizeRefreshPeriod)
	scaleSet.sizeMutex.Unlock()

	scaleSetStatusCache.mutex.Lock()
	scaleSetStatusCache.lastRefresh = time.Now().Add(-1 * scaleSet.sizeRefreshPeriod)
	scaleSetStatusCache.mutex.Unlock()
}

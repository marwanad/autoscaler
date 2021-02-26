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
	"fmt"
	"github.com/Azure/go-autorest/autorest/azure"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/services/containerservice/mgmt/2020-04-01/containerservice"
	"github.com/Azure/go-autorest/autorest/to"
	"k8s.io/apimachinery/pkg/api/resource"
	cloudvolume "k8s.io/cloud-provider/volume"
	klog "k8s.io/klog/v2"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/config/dynamic"
	kubeletapis "k8s.io/kubernetes/pkg/kubelet/apis"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

//AKSAgentPool implements NodeGroup interface for agent pool deployed in AKS
type AKSAgentPool struct {
	azureRef
	manager *AzureManager
	util    *AzUtil

	minSize           int
	maxSize           int
	clusterName       string
	resourceGroup     string
	nodeResourceGroup string
	location          string
	exists            bool
	autoprovisioned   bool
	spec              *autoprovisioningSpec

	curSize     int
	lastRefresh time.Time
	mutex       sync.Mutex
}

// Information about what autosprovisioning would like from this mig.
type autoprovisioningSpec struct {
	machineType    string
	labels         map[string]string
	taints         []apiv1.Taint
	extraResources map[string]resource.Quantity
}

//NewAKSAgentPool constructs AKSAgentPool from the --node param
//and azure manager
func NewAKSAgentPool(spec *dynamic.NodeGroupSpec, am *AzureManager, exists bool) (*AKSAgentPool, error) {
	asg := &AKSAgentPool{
		azureRef: azureRef{
			Name: spec.Name,
		},
		minSize:         spec.MinSize,
		maxSize:         spec.MaxSize,
		manager:         am,
		curSize:         -1,
		location:        am.config.Location,
		exists:          exists,
		autoprovisioned: !exists,
	}

	asg.util = &AzUtil{
		manager: am,
	}
	asg.clusterName = am.config.ClusterName
	asg.resourceGroup = am.config.ResourceGroup
	asg.nodeResourceGroup = am.config.NodeResourceGroup

	return asg, nil
}

//GetAKSAgentPool is an internal function which figures out ManagedClusterAgentPoolProfile from the list based on the pool name provided in the --node parameter passed
//to the autoscaler main
func (agentPool *AKSAgentPool) GetAKSAgentPool(agentProfiles *[]containerservice.ManagedClusterAgentPoolProfile) (ret *containerservice.ManagedClusterAgentPoolProfile) {
	for _, value := range *agentProfiles {
		profileName := *value.Name
		klog.V(5).Infof("AKS AgentPool profile name: %s", profileName)
		if strings.EqualFold(profileName, agentPool.azureRef.Name) {
			return &value
		}
	}

	return nil
}

// getAKSNodeCount gets node count for AKS agent pool.
func (agentPool *AKSAgentPool) getAKSNodeCount() (count int, err error) {
	if agentPool.exists {
		ctx, cancel := getContextWithCancel()
		defer cancel()

		managedCluster, rerr := agentPool.manager.azClient.managedKubernetesServicesClient.Get(ctx,
			agentPool.resourceGroup,
			agentPool.clusterName)
		if rerr != nil {
			klog.Errorf("Failed to get AKS cluster (name:%q): %v", agentPool.clusterName, rerr.Error())
			return -1, rerr.Error()
		}

		pool := agentPool.GetAKSAgentPool(managedCluster.AgentPoolProfiles)
		if pool == nil {
			return -1, fmt.Errorf("could not find pool with name: %s", agentPool.azureRef)
		}

		if pool.Count != nil {
			return int(*pool.Count), nil
		}
	}

	return 0, nil
}

// setAKSNodeCount sets node count for AKS agent pool.
func (agentPool *AKSAgentPool) setAKSNodeCount(count int) error {
	ctx, cancel := getContextWithCancel()
	defer cancel()

	managedCluster, rerr := agentPool.manager.azClient.managedKubernetesServicesClient.Get(ctx,
		agentPool.resourceGroup,
		agentPool.clusterName)
	if rerr != nil {
		klog.Errorf("Failed to get AKS cluster (name:%q): %v", agentPool.clusterName, rerr.Error())
		return rerr.Error()
	}

	pool := agentPool.GetAKSAgentPool(managedCluster.AgentPoolProfiles)
	if pool == nil {
		return fmt.Errorf("could not find pool with name: %s", agentPool.azureRef)
	}

	klog.Infof("Current size: %d, Target size requested: %d", *pool.Count, count)

	updateCtx, updateCancel := getContextWithCancel()
	defer updateCancel()
	*pool.Count = int32(count)
	aksClient := agentPool.manager.azClient.managedKubernetesServicesClient
	rerr = aksClient.CreateOrUpdate(updateCtx, agentPool.resourceGroup,
		agentPool.clusterName, managedCluster, "")
	if rerr != nil {
		klog.Errorf("Failed to update AKS cluster (%q): %v", agentPool.clusterName, rerr.Error())
		return rerr.Error()
	}

	klog.Infof("aksClient.CreateOrUpdate for aks cluster %q succeeded", agentPool.clusterName)
	return nil
}

//GetNodeCount returns the count of nodes from the managed agent pool profile
func (agentPool *AKSAgentPool) GetNodeCount() (count int, err error) {
	return agentPool.getAKSNodeCount()
}

//SetNodeCount sets the count of nodes in the in memory pool profile
func (agentPool *AKSAgentPool) SetNodeCount(count int) (err error) {
	return agentPool.setAKSNodeCount(count)
}

//GetProviderID converts the name of a node into the form that kubernetes cloud
//provider id is presented in.
func (agentPool *AKSAgentPool) GetProviderID(name string) string {
	//TODO: come with a generic way to make it work with provider id formats
	// in different version of k8s.
	return "azure://" + name
}

//GetName extracts the name of the node (a format which underlying cloud service understands)
//from the cloud providerID (format which kubernetes understands)
func (agentPool *AKSAgentPool) GetName(providerID string) (string, error) {
	// Remove the "azure://" string from it
	providerID = strings.TrimPrefix(providerID, "azure://")
	ctx, cancel := getContextWithCancel()
	defer cancel()
	vms, rerr := agentPool.manager.azClient.virtualMachinesClient.List(ctx, agentPool.nodeResourceGroup)
	if rerr != nil {
		return "", rerr.Error()
	}
	for _, vm := range vms {
		if strings.EqualFold(*vm.ID, providerID) {
			return *vm.Name, nil
		}
	}
	return "", fmt.Errorf("VM list empty")
}

//MaxSize returns the maximum size scale limit provided by --node
//parameter to the autoscaler main
func (agentPool *AKSAgentPool) MaxSize() int {
	return agentPool.maxSize
}

//MinSize returns the minimum size the cluster is allowed to scaled down
//to as provided by the node spec in --node parameter.
func (agentPool *AKSAgentPool) MinSize() int {
	return agentPool.minSize
}

//TargetSize gathers the target node count set for the cluster by
//querying the underlying service.
func (agentPool *AKSAgentPool) TargetSize() (int, error) {
	agentPool.mutex.Lock()
	defer agentPool.mutex.Unlock()

	if agentPool.exists {
		if agentPool.lastRefresh.Add(15 * time.Second).After(time.Now()) {
			return agentPool.curSize, nil
		}

		count, err := agentPool.GetNodeCount()
		if err != nil {
			return -1, err
		}
		klog.V(5).Infof("Got new size %d for agent pool (%q)", count, agentPool.Name)

		agentPool.curSize = count
		agentPool.lastRefresh = time.Now()
	}

	return agentPool.curSize, nil
}

//SetSize contacts the underlying service and sets the size of the pool.
//This will be called when a scale up occurs and will be called just after
//a delete is performed from a scale down.
func (agentPool *AKSAgentPool) SetSize(targetSize int, isScalingDown bool) (err error) {
	agentPool.mutex.Lock()
	defer agentPool.mutex.Unlock()

	return agentPool.setSizeInternal(targetSize, isScalingDown)
}

// setSizeInternal contacts the underlying service and sets the size of the pool.
// It should be called under lock protected.
func (agentPool *AKSAgentPool) setSizeInternal(targetSize int, isScalingDown bool) (err error) {
	if isScalingDown && targetSize < agentPool.MinSize() {
		klog.Errorf("size-decreasing request of %d is smaller than min size %d", targetSize, agentPool.MinSize())
		return fmt.Errorf("size-decreasing request of %d is smaller than min size %d", targetSize, agentPool.MinSize())
	}

	klog.V(2).Infof("Setting size for cluster (%q) with new count (%d)", agentPool.clusterName, targetSize)

	if agentPool.exists {
		err = agentPool.setAKSNodeCount(targetSize)
		if err != nil {
			return err
		}
	}

	agentPool.curSize = targetSize
	agentPool.lastRefresh = time.Now()
	return nil
}

//IncreaseSize calls in the underlying SetSize to increase the size in response
//to a scale up. It calculates the expected size based on a delta provided as
//parameter
func (agentPool *AKSAgentPool) IncreaseSize(delta int) error {
	if delta <= 0 {
		return fmt.Errorf("size increase must be +ve")
	}
	currentSize, err := agentPool.TargetSize()
	if err != nil {
		return err
	}
	targetSize := currentSize + delta
	if targetSize > agentPool.MaxSize() {
		return fmt.Errorf("size-increasing request of %d is bigger than max size %d", targetSize, agentPool.MaxSize())
	}
	return agentPool.SetSize(targetSize, false)
}

// deleteNodesInternal calls the underlying vm service to delete the node.
// It should be called within lock protected.
func (agentPool *AKSAgentPool) deleteNodesInternal(providerIDs []string) (deleted int, err error) {
	for _, providerID := range providerIDs {
		klog.Infof("ProviderID got to delete: %s", providerID)
		nodeName, err := agentPool.GetName(providerID)
		if err != nil {
			return deleted, err
		}
		klog.Infof("VM name got to delete: %s", nodeName)

		err = agentPool.util.DeleteVirtualMachine(agentPool.nodeResourceGroup, nodeName)
		if err != nil {
			klog.Errorf("Failed to delete virtual machine %q with error: %v", nodeName, err)
			return deleted, err
		}

		// increase the deleted count after delete VM succeed.
		deleted++
	}

	return deleted, nil
}

//DeleteNodes extracts the providerIDs from the node spec and calls into the internal
//delete method.
func (agentPool *AKSAgentPool) DeleteNodes(nodes []*apiv1.Node) error {
	agentPool.mutex.Lock()
	defer agentPool.mutex.Unlock()

	var providerIDs []string
	for _, node := range nodes {
		providerIDs = append(providerIDs, node.Spec.ProviderID)
	}

	deleted, deleteError := agentPool.deleteNodesInternal(providerIDs)
	// Update node count if there're some virtual machines got deleted.
	if deleted != 0 {
		targetSize := agentPool.curSize - deleted
		err := agentPool.setSizeInternal(targetSize, true)
		if err != nil {
			klog.Errorf("Failed to set size for agent pool %q with error: %v", agentPool.Name, err)
		} else {
			klog.V(3).Infof("Size for agent pool %q has been updated to %d", agentPool.Name, targetSize)
		}
	}
	return deleteError
}

//IsAKSNode checks if the tag from the vm matches the agentPool name
func (agentPool *AKSAgentPool) IsAKSNode(tags map[string]*string) bool {
	poolName := tags["poolName"]
	if poolName != nil {
		klog.V(5).Infof("Matching agentPool name: %s with tag name: %s", agentPool.azureRef.Name, *poolName)
		if strings.EqualFold(*poolName, agentPool.azureRef.Name) {
			return true
		}
	}
	return false
}

//GetNodes extracts the node list from the underlying vm service and returns back
//equivalent providerIDs  as list.
func (agentPool *AKSAgentPool) GetNodes() ([]string, error) {
	ctx, cancel := getContextWithCancel()
	defer cancel()
	klog.V(6).Infof("GetNodes: starting list aks node pools in %s", agentPool.nodeResourceGroup)
	vmList, rerr := agentPool.manager.azClient.virtualMachinesClient.List(ctx, agentPool.nodeResourceGroup)
	klog.V(6).Infof("GetNodes: list finished, len(vmlist) = %d, err = %s", len(vmList), rerr.Error())
	if rerr != nil {
		klog.Errorf("Azure client list vm error : %v", rerr.Error())
		return nil, rerr.Error()
	}
	var nodeArray []string
	for _, node := range vmList {
		klog.V(5).Infof("Node Name: %s, ID: %s", *node.Name, *node.ID)
		if agentPool.IsAKSNode(node.Tags) {
			providerID, err := convertResourceGroupNameToLower(agentPool.GetProviderID(*node.ID))
			if err != nil {
				// This shouldn't happen. Log a waring message for tracking.
				klog.Warningf("GetNodes.convertResourceGroupNameToLower failed with error: %v", err)
				continue
			}

			klog.V(5).Infof("Returning back the providerID: %s", providerID)
			nodeArray = append(nodeArray, providerID)
		}
	}
	return nodeArray, nil
}

//DecreaseTargetSize requests the underlying service to decrease the node count.
func (agentPool *AKSAgentPool) DecreaseTargetSize(delta int) error {
	if delta >= 0 {
		klog.Errorf("Size decrease error: %d", delta)
		return fmt.Errorf("size decrease must be negative")
	}
	currentSize, err := agentPool.TargetSize()
	if err != nil {
		klog.Error(err)
		return err
	}
	klog.V(5).Infof("DecreaseTargetSize get current size %d for agent pool %q", currentSize, agentPool.Name)

	// Get the current nodes in the list
	nodes, err := agentPool.GetNodes()
	if err != nil {
		klog.Error(err)
		return err
	}

	targetSize := currentSize + delta
	klog.V(5).Infof("DecreaseTargetSize get target size %d for agent pool %q", targetSize, agentPool.Name)
	if targetSize < len(nodes) {
		return fmt.Errorf("attempt to delete existing nodes targetSize:%d delta:%d existingNodes: %d",
			currentSize, delta, len(nodes))
	}
	return agentPool.SetSize(targetSize, true)
}

//Id returns the name of the agentPool
func (agentPool *AKSAgentPool) Id() string {
	return agentPool.azureRef.Name
}

//Debug returns a string with basic details of the agentPool
func (agentPool *AKSAgentPool) Debug() string {
	return fmt.Sprintf("%s (%d:%d)", agentPool.Id(), agentPool.MinSize(), agentPool.MaxSize())
}

//Nodes returns the list of nodes in the agentPool.
func (agentPool *AKSAgentPool) Nodes() ([]cloudprovider.Instance, error) {
	instanceNames, err := agentPool.GetNodes()
	if err != nil {
		return nil, err
	}
	instances := make([]cloudprovider.Instance, 0, len(instanceNames))
	for _, instanceName := range instanceNames {
		instances = append(instances, cloudprovider.Instance{Id: instanceName})
	}
	return instances, nil
}

//TemplateNodeInfo is not implemented.
func (agentPool *AKSAgentPool) TemplateNodeInfo() (*schedulerframework.NodeInfo, error) {
	if agentPool.exists {
		ctx, cancel := getContextWithCancel()
		defer cancel()

		managedCluster, rerr := agentPool.manager.azClient.managedKubernetesServicesClient.Get(ctx,
			agentPool.resourceGroup,
			agentPool.clusterName)
		if rerr != nil {
			klog.Errorf("Failed to get AKS cluster (name:%q): %v", agentPool.clusterName, rerr.Error())
			return nil, rerr.Error()
		}

		// TODO(ace): this should not fetch from AKS. It needs to use data
		// stored in the struct. Location in particular needs to be predefined.
		pool := agentPool.GetAKSAgentPool(managedCluster.AgentPoolProfiles)
		if pool == nil {
			return nil, fmt.Errorf("could not find pool with name: %s", agentPool.azureRef)
		}

		isWindows := strings.EqualFold(string(pool.OsType), "windows")

		node, err := buildNodeFromTemplate(agentPool.azureRef.Name, string(pool.VMSize), *managedCluster.Location, pool.AvailabilityZones, isWindows, pool.Tags)
		if err != nil {
			return nil, err
		}

		nodeInfo := schedulerframework.NewNodeInfo(cloudprovider.BuildKubeProxy(agentPool.azureRef.Name))
		nodeInfo.SetNode(node)
		return nodeInfo, nil
	} else if agentPool.spec != nil {
		var zones *[]string
		zoneString := agentPool.spec.labels[apiv1.LabelZoneFailureDomain]
		if len(zoneString) > 0 {
			zs := strings.Split(zoneString, cloudvolume.LabelMultiZoneDelimiter)
			zones = &zs
		}
		osLabel := agentPool.spec.labels[kubeletapis.LabelOS]
		osLabelStable := agentPool.spec.labels[apiv1.LabelOSStable]
		if osLabel != "" && osLabelStable != "" && osLabel != osLabelStable {
			return nil, fmt.Errorf("label '%s=%s' and label '%s=%s' must match, and they do not",
				kubeletapis.LabelOS,
				osLabel,
				apiv1.LabelOSStable,
				osLabelStable,
			)
		}

		isWindows := strings.EqualFold(osLabel, "windows")

		node, err := buildNodeFromTemplate(agentPool.azureRef.Name, agentPool.spec.machineType, agentPool.location, zones, isWindows, nil)
		if err != nil {
			return nil, err
		}

		nodeInfo := schedulerframework.NewNodeInfo(cloudprovider.BuildKubeProxy(agentPool.azureRef.Name))
		nodeInfo.SetNode(node)
		return nodeInfo, nil
	}

	return nil, cloudprovider.ErrNotImplemented
}

//Exist is always true since we are initialized with an existing agentpool
func (agentPool *AKSAgentPool) Exist() bool {
	return agentPool.exists
}

// Create triggers creation of autoprovisioned nodepools on AKS side.
// not implemented for other vm types.
func (agentPool *AKSAgentPool) Create(nodeCount int) (cloudprovider.NodeGroup, error) {
	ap := agentPool
	if !ap.exists && ap.autoprovisioned {
		ctx, cancel := getContextWithCancel()
		defer cancel()

		managedCluster, rerr := ap.manager.azClient.managedKubernetesServicesClient.Get(ctx, ap.resourceGroup, ap.clusterName)
		if rerr != nil {
			klog.Errorf("Failed to get AKS cluster (name:%s): %s", ap.clusterName, rerr.Error())
			return nil, rerr.Error()
		}

		labels := map[string]*string{}
		taints := []string{}
		for k, v := range ap.spec.labels {
			labels[k] = &v
		}
		for _, v := range ap.spec.taints {
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
				VMSize:              containerservice.VMSizeTypes(ap.spec.machineType),
				OsType:              containerservice.Linux,
				Count:               to.Int32Ptr(int32(nodeCount)),
				Type:                containerservice.VirtualMachineScaleSets,
				OrchestratorVersion: managedCluster.ManagedClusterProperties.KubernetesVersion,
				Tags: map[string]*string{
					"autoprovision": to.StringPtr("true"),
				},
				NodeLabels: labels,
				NodeTaints: &taints,
			},
		}

		updateCtx, updateCancel := getContextWithCancel()
		defer updateCancel()
		aksClient := ap.manager.azClient.agentPoolsClient

		future, rerr := aksClient.CreateOrUpdateAsync(updateCtx, ap.resourceGroup, ap.clusterName, ap.azureRef.Name, pool, "")
		if rerr != nil {
			klog.Errorf("Failed to create AKS nodepool (%q) - %d for cluster %q: %v", ap.Name, int32(ap.curSize), ap.clusterName, rerr.Error())
			return nil, rerr.Error()
		}
		// Proactively register group so CA core gets the upcoming node info
		// and back-off
		ap.manager.RegisterNodeGroup(ap)
		ap.exists = true
		go ap.waitForCreateOrUpdateAp(future)

		return ap, nil
	}
	return nil, cloudprovider.ErrAlreadyExist
}

func (ap *AKSAgentPool) waitForCreateOrUpdateAp(future *azure.Future) {
	var err error
	aksClient := ap.manager.azClient.agentPoolsClient

	defer func() {
		if err != nil {
		}
	}()

	ctx, cancel := getContextWithCancel()
	defer cancel()

	klog.V(3).Infof("Calling aksClient.WaitForCreateOrUpdateResult(%s)", ap.azureRef.Name)
	httpResponse, err := aksClient.WaitForCreateOrUpdateResult(ctx, future, ap.resourceGroup)

	isSuccess, err := isSuccessHTTPResponse(httpResponse, err)
	if isSuccess {
		klog.V(3).Infof("aksClient.WaitForCreateOrUpdateResult(%s) success", ap.azureRef.Name)
	} else {
		klog.Errorf("Failed to create or update agentpool", ap.Name, err)
	}
	// TODO: think if this is thread-safe when i have more brain cells
	ap.manager.invalidateCache()
}

// Delete deletes autoprovisioned node pools. Not supported otherwise.
func (agentPool *AKSAgentPool) Delete() error {
	ap := agentPool
	if ap.autoprovisioned {
		updateCtx, updateCancel := getContextWithCancel()
		defer updateCancel()

		aksClient := ap.manager.azClient.agentPoolsClient
		err := aksClient.Delete(updateCtx, ap.resourceGroup, ap.clusterName, ap.azureRef.Name)
		if err != nil {
			klog.Errorf("Failed to delete AKS nodepool (%q): %v", ap.clusterName, err.Error())
			return err.Error()
		}

		ap.manager.UnregisterNodeGroup(ap)
		ap.exists = false

		return nil
	}

	return cloudprovider.ErrNotImplemented
}

//Autoprovisioned is set to false to indicate that this code
//does not create agentPools by itself.
func (agentPool *AKSAgentPool) Autoprovisioned() bool {
	return false
}

// GetOptions returns NodeGroupAutoscalingOptions that should be used for this particular
// NodeGroup. Returning a nil will result in using default options.
func (agentPool *AKSAgentPool) GetOptions(defaults config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	return nil, cloudprovider.ErrNotImplemented
}

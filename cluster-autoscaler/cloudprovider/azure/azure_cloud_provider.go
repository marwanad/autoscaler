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
	"hash/fnv"
	"io"
	"k8s.io/autoscaler/cluster-autoscaler/config/dynamic"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"math/rand"
	"os"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	klog "k8s.io/klog/v2"
)

const (
	// GPULabel is the label added to nodes with GPU resource.
	GPULabel = "accelerator"
)

var (
	availableGPUTypes = map[string]struct{}{
		"nvidia-tesla-k80":  {},
		"nvidia-tesla-p100": {},
		"nvidia-tesla-v100": {},
	}
)

// AzureCloudProvider provides implementation of CloudProvider interface for Azure.
type AzureCloudProvider struct {
	azureManager    *AzureManager
	resourceLimiter *cloudprovider.ResourceLimiter
}

// BuildAzureCloudProvider creates new AzureCloudProvider
func BuildAzureCloudProvider(azureManager *AzureManager, resourceLimiter *cloudprovider.ResourceLimiter) (cloudprovider.CloudProvider, error) {
	azure := &AzureCloudProvider{
		azureManager:    azureManager,
		resourceLimiter: resourceLimiter,
	}

	return azure, nil
}

// Cleanup stops the go routine that is handling the current view of the ASGs in the form of a cache
func (azure *AzureCloudProvider) Cleanup() error {
	azure.azureManager.Cleanup()
	return nil
}

// Name returns name of the cloud provider.
func (azure *AzureCloudProvider) Name() string {
	return "azure"
}

// GPULabel returns the label added to nodes with GPU resource.
func (azure *AzureCloudProvider) GPULabel() string {
	return GPULabel
}

// GetAvailableGPUTypes return all available GPU types cloud provider supports
func (azure *AzureCloudProvider) GetAvailableGPUTypes() map[string]struct{} {
	return availableGPUTypes
}

// NodeGroups returns all node groups configured for this cloud provider.
func (azure *AzureCloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	asgs := azure.azureManager.getAsgs()

	ngs := make([]cloudprovider.NodeGroup, len(asgs))
	for i, asg := range asgs {
		ngs[i] = asg
	}
	return ngs
}

// NodeGroupForNode returns the node group for the given node.
func (azure *AzureCloudProvider) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	klog.V(6).Infof("NodeGroupForNode: starts")
	if node.Spec.ProviderID == "" {
		klog.V(6).Infof("Skipping the search for node group for the node '%s' because it has no autoProvisioningSpec.ProviderID", node.ObjectMeta.Name)
		return nil, nil
	}
	klog.V(6).Infof("Searching for node group for the node: %s\n", node.Spec.ProviderID)
	ref := &azureRef{
		Name: node.Spec.ProviderID,
	}

	klog.V(6).Infof("NodeGroupForNode: ref.Name %s", ref.Name)
	return azure.azureManager.GetAsgForInstance(ref)
}

// Pricing returns pricing model for this cloud provider or error if not available.
func (azure *AzureCloudProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	return nil, cloudprovider.ErrNotImplemented
}

// GetAvailableMachineTypes get all machine types that can be requested from the cloud provider.
// TODO: restrict those to few SKUS and a GPU one
func (azure *AzureCloudProvider) GetAvailableMachineTypes() ([]string, error) {
	names := make([]string, 0, len(InstanceTypes))
	for name, val := range InstanceTypes {
		if strings.HasPrefix(name, "Standard_D") && strings.HasSuffix(name, "s_v3") && !strings.HasSuffix(name, "as_v3") && !strings.HasSuffix(name, "as_v4") && !strings.HasSuffix(name, "_v2") && !strings.HasSuffix(name, "_Promo") {
			if val.VCPU > 2 {
				names = append(names, name)
			}
		}
	}
	return names, nil
}

// NewNodeGroup builds a theoretical node group based on the node definition provided. The node group is not automatically
// created on the cloud provider side. The node group is not returned by NodeGroups() until it is created.
func (azure *AzureCloudProvider) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string,
	taints []apiv1.Taint, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	uniqueNameSuffixSize := 8
	h := fnv.New64a()
	h.Write([]byte(machineType))
	r := rand.New(rand.NewSource(int64(h.Sum64())))
	nodePoolName := fmt.Sprintf("a%08d\n", r.Uint32())[:uniqueNameSuffixSize]

	if gpuRequest, found := extraResources[gpu.ResourceNvidiaGPU]; found {
		gpuType, found := systemLabels[GPULabel]
		if !found {
			return nil, cloudprovider.ErrIllegalConfiguration
		}
		gpuCount, err := getNormalizedGpuCount(gpuRequest.Value())
		if err != nil {
			return nil, err
		}
		extraResources[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(gpuCount, resource.DecimalSI)
		labels[GPULabel] = gpuType

		taint := apiv1.Taint{
			Effect: apiv1.TaintEffectNoSchedule,
			Key:    GPULabel,
			Value:  gpuType,
		}
		taints = append(taints, taint)
	}

	// TODO(ace): change min/max
	spec := &dynamic.NodeGroupSpec{
		Name:               nodePoolName,
		MinSize:            0,
		MaxSize:            100,
		SupportScaleToZero: true,
	}

	ng, err := NewScaleSet(spec, azure.azureManager, 0, false, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create new node group: %w", err)
	}

	ng.autoProvisioningSpec = &autoprovisioningSpec{
		machineType:    machineType,
		labels:         labels,
		extraResources: extraResources,
	}

	return ng, nil
}

func getNormalizedGpuCount(initialCount int64) (int64, error) {
	for i := int64(1); i <= int64(8); i = 2 * i {
		if i >= initialCount {
			return i, nil
		}
	}
	return 0, cloudprovider.ErrIllegalConfiguration
}

// GetResourceLimiter returns struct containing limits (max, min) for resources (cores, memory etc.).
func (azure *AzureCloudProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	return azure.resourceLimiter, nil
}

// Refresh is called before every main loop and can be used to dynamically update cloud provider state.
// In particular the list of node groups returned by NodeGroups can change as a result of CloudProvider.Refresh().
func (azure *AzureCloudProvider) Refresh() error {
	return azure.azureManager.Refresh()
}

// azureRef contains a reference to some entity in Azure world.
type azureRef struct {
	Name string
}

// GetKey returns key of the given azure reference.
func (m *azureRef) GetKey() string {
	return m.Name
}

// String is represented by calling GetKey()
func (m *azureRef) String() string {
	return m.GetKey()
}

// BuildAzure builds Azure cloud provider, manager etc.
func BuildAzure(opts config.AutoscalingOptions, do cloudprovider.NodeGroupDiscoveryOptions, rl *cloudprovider.ResourceLimiter) cloudprovider.CloudProvider {
	var config io.ReadCloser
	if opts.CloudConfig != "" {
		klog.Infof("Creating Azure Manager using cloud-config file: %v", opts.CloudConfig)
		var err error
		config, err = os.Open(opts.CloudConfig)
		if err != nil {
			klog.Fatalf("Couldn't open cloud provider configuration %s: %#v", opts.CloudConfig, err)
		}
		defer config.Close()
	} else {
		klog.Info("Creating Azure Manager with default configuration.")
	}
	manager, err := CreateAzureManager(config, do)
	if err != nil {
		klog.Fatalf("Failed to create Azure Manager: %v", err)
	}
	provider, err := BuildAzureCloudProvider(manager, rl)
	if err != nil {
		klog.Fatalf("Failed to create Azure cloud provider: %v", err)
	}
	return provider
}

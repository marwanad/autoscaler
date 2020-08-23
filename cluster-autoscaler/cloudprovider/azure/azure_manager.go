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

//go:generate go run azure_instance_types/gen.go

package azure

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/go-autorest/autorest/azure"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config/dynamic"
	klog "k8s.io/klog/v2"
)

const (
	vmTypeVMSS     = "vmss"
	vmTypeStandard = "standard"
	vmTypeAKS      = "aks"

	scaleToZeroSupportedStandard = false
	scaleToZeroSupportedVMSS     = true
	refreshInterval              = 1 * time.Minute

	// The path of deployment parameters for standard vm.
	deploymentParametersPath = "/var/lib/azure/azuredeploy.parameters.json"

	vmssTagMin                     = "min"
	vmssTagMax                     = "max"
	autoDiscovererTypeLabel        = "label"
	labelAutoDiscovererKeyMinNodes = "min"
	labelAutoDiscovererKeyMaxNodes = "max"
	metadataURL                    = "http://169.254.169.254/metadata/instance"

	// backoff
	backoffRetriesDefault  = 6
	backoffExponentDefault = 1.5
	backoffDurationDefault = 5 // in seconds
	backoffJitterDefault   = 1.0

	// rate limit
	rateLimitQPSDefault         float32 = 1.0
	rateLimitBucketDefault              = 5
	rateLimitReadQPSEnvVar              = "RATE_LIMIT_READ_QPS"
	rateLimitReadBucketsEnvVar          = "RATE_LIMIT_READ_BUCKETS"
	rateLimitWriteQPSEnvVar             = "RATE_LIMIT_WRITE_QPS"
	rateLimitWriteBucketsEnvVar         = "RATE_LIMIT_WRITE_BUCKETS"
)

var validLabelAutoDiscovererKeys = strings.Join([]string{
	labelAutoDiscovererKeyMinNodes,
	labelAutoDiscovererKeyMaxNodes,
}, ", ")

// A labelAutoDiscoveryConfig specifies how to autodiscover Azure scale sets.
type labelAutoDiscoveryConfig struct {
	// Key-values to match on.
	Selector map[string]string
}

// AzureManager handles Azure communication and data caching.
type AzureManager struct {
	config   *Config
	azClient *azClient
	env      azure.Environment

	azCache               *azureCache
	lastRefresh           time.Time
	asgAutoDiscoverySpecs []labelAutoDiscoveryConfig
	explicitlyConfigured  map[string]bool
}



// CreateAzureManager creates Azure Manager object to work with Azure.
func CreateAzureManager(configReader io.Reader, discoveryOpts cloudprovider.NodeGroupDiscoveryOptions) (*AzureManager, error) {
	cfg, err := BuildAzureConfig(configReader)
	if err != nil {
		return nil, err
	}

	// Defaulting env to Azure Public Cloud.
	env := azure.PublicCloud
	if cfg.Cloud != "" {
		env, err = azure.EnvironmentFromName(cfg.Cloud)
		if err != nil {
			return nil, err
		}
	}

	klog.Infof("Starting azure manager with subscription ID %q", cfg.SubscriptionID)

	azClient, err := newAzClient(cfg, &env)
	if err != nil {
		return nil, err
	}

	// Create azure manager.
	manager := &AzureManager{
		config:               cfg,
		env:                  env,
		azClient:             azClient,
		explicitlyConfigured: make(map[string]bool),
	}

	manager.azCache = NewAzureCache()

	specs, err := parseLabelAutoDiscoverySpecs(discoveryOpts)
	if err != nil {
		return nil, err
	}
	manager.asgAutoDiscoverySpecs = specs

	if err := manager.fetchExplicitAsgs(discoveryOpts.NodeGroupSpecs); err != nil {
		return nil, err
	}

	if err := manager.forceRefresh(); err != nil {
		return nil, err
	}

	return manager, nil
}

func (m *AzureManager) fetchExplicitAsgs(specs []string) error {
	changed := false
	for _, spec := range specs {
		asg, err := m.buildAsgFromSpec(spec)
		if err != nil {
			return fmt.Errorf("failed to parse node group spec: %v", err)
		}
		if m.RegisterAsg(asg) {
			changed = true
		}
		m.explicitlyConfigured[asg.Id()] = true
	}

	if changed {
		if err := m.azCache.RegenerateInstanceCache(); err != nil {
			return err
		}
	}
	return nil
}

func (m *AzureManager) buildAsgFromSpec(spec string) (cloudprovider.NodeGroup, error) {
	scaleToZeroSupported := scaleToZeroSupportedStandard
	if strings.EqualFold(m.config.VMType, vmTypeVMSS) {
		scaleToZeroSupported = scaleToZeroSupportedVMSS
	}
	s, err := dynamic.SpecFromString(spec, scaleToZeroSupported)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node group spec: %v", err)
	}

	switch m.config.VMType {
	case vmTypeStandard:
		return NewAgentPool(s, m)
	case vmTypeVMSS:
		return NewScaleSet(s, m, -1)
	case vmTypeAKS:
		return NewAKSAgentPool(s, m)
	default:
		return nil, fmt.Errorf("vmtype %s not supported", m.config.VMType)
	}
}

// Refresh is called before every main loop and can be used to dynamically update cloud provider state.
// In particular the list of node groups returned by NodeGroups can change as a result of CloudProvider.Refresh().
func (m *AzureManager) Refresh() error {
	if m.lastRefresh.Add(refreshInterval).After(time.Now()) {
		return nil
	}
	return m.forceRefresh()
}

func (m *AzureManager) forceRefresh() error {
	// TODO: Refactor some of this logic out of forceRefresh and
	// consider merging the list call with the Nodes() call
	if err := m.fetchAutoAsgs(); err != nil {
		klog.Errorf("Failed to fetch ASGs: %v", err)
	}
	if err := m.azCache.RegenerateInstanceCache(); err != nil {
		klog.Errorf("Failed to regenerate cache: %v", err)
		return err
	}
	m.lastRefresh = time.Now()
	klog.V(2).Infof("Refreshed ASG list, next refresh after %v", m.lastRefresh.Add(refreshInterval))
	return nil
}

// Fetch automatically discovered ASGs. These ASGs should be unregistered if
// they no longer exist in Azure.
func (m *AzureManager) fetchAutoAsgs() error {
	groups, err := m.getFilteredAutoscalingGroups(m.asgAutoDiscoverySpecs)
	if err != nil {
		return fmt.Errorf("cannot autodiscover ASGs: %s", err)
	}

	changed := false
	exists := make(map[string]bool)
	for _, asg := range groups {
		asgID := asg.Id()
		exists[asgID] = true
		if m.explicitlyConfigured[asgID] {
			// This ASG was explicitly configured, but would also be
			// autodiscovered. We want the explicitly configured min and max
			// nodes to take precedence.
			klog.V(3).Infof("Ignoring explicitly configured ASG %s for autodiscovery.", asg.Id())
			continue
		}
		if m.RegisterAsg(asg) {
			klog.V(3).Infof("Autodiscovered ASG %s using tags %v", asg.Id(), m.asgAutoDiscoverySpecs)
			changed = true
		}
	}

	for _, asg := range m.azCache.getNodeGroups() {
		asgID := asg.Id()
		if !exists[asgID] && !m.explicitlyConfigured[asgID] {
			m.UnregisterAsg(asg)
			changed = true
		}
	}

	if changed {
		if err := m.azCache.RegenerateInstanceCache(); err != nil {
			return err
		}
	}

	return nil
}

// RegisterAsg registers an ASG.
func (m *AzureManager) RegisterAsg(asg cloudprovider.NodeGroup) bool {
	return m.azCache.Register(asg)
}

// UnregisterAsg unregisters an ASG.
func (m *AzureManager) UnregisterAsg(asg cloudprovider.NodeGroup) bool {
	return m.azCache.Unregister(asg)
}

// GetAsgForInstance returns AsgConfig of the given Instance
func (m *AzureManager) GetAsgForInstance(instance *azureRef) (cloudprovider.NodeGroup, error) {
	return m.azCache.GetNodeGroupForInstance(instance, m.config.VMType)
}

// Cleanup the ASG cache.
func (m *AzureManager) Cleanup() {
	m.azCache.Cleanup()
}

func (m *AzureManager) getFilteredAutoscalingGroups(filter []labelAutoDiscoveryConfig) (asgs []cloudprovider.NodeGroup, err error) {
	if len(filter) == 0 {
		return nil, nil
	}

	switch m.config.VMType {
	case vmTypeVMSS:
		asgs, err = m.listScaleSets(filter)
	case vmTypeStandard:
		asgs, err = m.listAgentPools(filter)
	case vmTypeAKS:
		return nil, nil
	default:
		err = fmt.Errorf("vmType %q not supported", m.config.VMType)
	}
	if err != nil {
		return nil, err
	}

	return asgs, nil
}

// listScaleSets gets a list of scale sets and instanceIDs.
func (m *AzureManager) listScaleSets(filter []labelAutoDiscoveryConfig) ([]cloudprovider.NodeGroup, error) {
	ctx, cancel := getContextWithCancel()
	defer cancel()

	result, rerr := m.azClient.virtualMachineScaleSetsClient.List(ctx, m.config.ResourceGroup)
	if rerr != nil {
		klog.Errorf("VirtualMachineScaleSetsClient.List for %v failed: %v", m.config.ResourceGroup, rerr)
		return nil, rerr.Error()
	}

	var asgs []cloudprovider.NodeGroup
	for _, scaleSet := range result {
		if len(filter) > 0 {
			if scaleSet.Tags == nil || len(scaleSet.Tags) == 0 {
				continue
			}

			if !matchDiscoveryConfig(scaleSet.Tags, filter) {
				continue
			}
		}
		spec := &dynamic.NodeGroupSpec{
			Name:               *scaleSet.Name,
			MinSize:            1,
			MaxSize:            -1,
			SupportScaleToZero: scaleToZeroSupportedVMSS,
		}

		if val, ok := scaleSet.Tags["min"]; ok {
			if minSize, err := strconv.Atoi(*val); err == nil {
				spec.MinSize = minSize
			} else {
				klog.Warningf("ignoring nodegroup %q because of invalid minimum size specified for vmss: %s", *scaleSet.Name, err)
				continue
			}
		} else {
			klog.Warningf("ignoring nodegroup %q because of no minimum size specified for vmss", *scaleSet.Name)
			continue
		}
		if spec.MinSize < 0 {
			klog.Warningf("ignoring nodegroup %q because of minimum size must be a non-negative number of nodes", *scaleSet.Name)
			continue
		}
		if val, ok := scaleSet.Tags["max"]; ok {
			if maxSize, err := strconv.Atoi(*val); err == nil {
				spec.MaxSize = maxSize
			} else {
				klog.Warningf("ignoring nodegroup %q because of invalid maximum size specified for vmss: %s", *scaleSet.Name, err)
				continue
			}
		} else {
			klog.Warningf("ignoring nodegroup %q because of no maximum size specified for vmss", *scaleSet.Name)
			continue
		}
		if spec.MaxSize < spec.MinSize {
			klog.Warningf("ignoring nodegroup %q because of maximum size must be greater than minimum size: max=%d < min=%d", *scaleSet.Name, spec.MaxSize, spec.MinSize)
			continue
		}

		curSize := int64(-1)
		if scaleSet.Sku != nil && scaleSet.Sku.Capacity != nil {
			curSize = *scaleSet.Sku.Capacity
		}

		asg, err := NewScaleSet(spec, m, curSize)
		if err != nil {
			klog.Warningf("ignoring nodegroup %q %s", *scaleSet.Name, err)
			continue
		}
		asgs = append(asgs, asg)
	}

	return asgs, nil
}

// listAgentPools gets a list of agent pools and instanceIDs.
// Note: filter won't take effect for agent pools.
func (m *AzureManager) listAgentPools(filter []labelAutoDiscoveryConfig) (asgs []cloudprovider.NodeGroup, err error) {
	ctx, cancel := getContextWithCancel()
	defer cancel()
	deploy, err := m.azClient.deploymentsClient.Get(ctx, m.config.ResourceGroup, m.config.Deployment)
	if err != nil {
		klog.Errorf("deploymentsClient.Get(%s, %s) failed: %v", m.config.ResourceGroup, m.config.Deployment, err)
		return nil, err
	}

	parameters := deploy.Properties.Parameters.(map[string]interface{})
	for k := range parameters {
		if k == "masterVMSize" || !strings.HasSuffix(k, "VMSize") {
			continue
		}

		poolName := strings.TrimRight(k, "VMSize")
		spec := &dynamic.NodeGroupSpec{
			Name:               poolName,
			MinSize:            1,
			MaxSize:            -1,
			SupportScaleToZero: scaleToZeroSupportedStandard,
		}
		asg, _ := NewAgentPool(spec, m)
		asgs = append(asgs, asg)
	}

	return asgs, nil
}

// ParseLabelAutoDiscoverySpecs returns any provided NodeGroupAutoDiscoverySpecs
// parsed into configuration appropriate for ASG autodiscovery.
func parseLabelAutoDiscoverySpecs(o cloudprovider.NodeGroupDiscoveryOptions) ([]labelAutoDiscoveryConfig, error) {
	cfgs := make([]labelAutoDiscoveryConfig, len(o.NodeGroupAutoDiscoverySpecs))
	var err error
	for i, spec := range o.NodeGroupAutoDiscoverySpecs {
		cfgs[i], err = parseLabelAutoDiscoverySpec(spec)
		if err != nil {
			return nil, err
		}
	}
	return cfgs, nil
}

// parseLabelAutoDiscoverySpec parses a single spec and returns the corredponding node group spec.
func parseLabelAutoDiscoverySpec(spec string) (labelAutoDiscoveryConfig, error) {
	cfg := labelAutoDiscoveryConfig{
		Selector: make(map[string]string),
	}

	tokens := strings.Split(spec, ":")
	if len(tokens) != 2 {
		return cfg, fmt.Errorf("spec \"%s\" should be discoverer:key=value,key=value", spec)
	}
	discoverer := tokens[0]
	if discoverer != autoDiscovererTypeLabel {
		return cfg, fmt.Errorf("unsupported discoverer specified: %s", discoverer)
	}

	for _, arg := range strings.Split(tokens[1], ",") {
		kv := strings.Split(arg, "=")
		if len(kv) != 2 {
			return cfg, fmt.Errorf("invalid key=value pair %s", kv)
		}
		k, v := kv[0], kv[1]
		if k == "" || v == "" {
			return cfg, fmt.Errorf("empty value not allowed in key=value tag pairs")
		}
		cfg.Selector[k] = v
	}
	return cfg, nil
}


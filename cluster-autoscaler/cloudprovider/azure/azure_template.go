/*
Copyright 2020 The Kubernetes Authors.

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
	"math/rand"
	"regexp"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	cloudvolume "k8s.io/cloud-provider/volume"
	"k8s.io/klog/v2"
	kubeletapis "k8s.io/kubernetes/pkg/kubelet/apis"
)

func buildInstanceOS(windows bool) string {
	instanceOS := cloudprovider.DefaultOS
	// if template.VirtualMachineProfile != nil && template.VirtualMachineProfile.OsProfile != nil && template.VirtualMachineProfile.OsProfile.WindowsConfiguration != nil {
	if windows {
		instanceOS = "windows"
	}

	return instanceOS
}

func buildGenericLabels(vmSizeName, nodeName, location string, zones *[]string, windows bool) map[string]string {
	result := make(map[string]string)

	result[kubeletapis.LabelArch] = cloudprovider.DefaultArch
	result[apiv1.LabelArchStable] = cloudprovider.DefaultArch

	result[kubeletapis.LabelOS] = buildInstanceOS(windows)
	result[apiv1.LabelOSStable] = buildInstanceOS(windows)

	result[apiv1.LabelInstanceType] = vmSizeName
	result[apiv1.LabelZoneRegion] = strings.ToLower(location)

	if zones != nil && len(*zones) > 0 {
		failureDomains := make([]string, len(*zones))
		for k, v := range *zones {
			failureDomains[k] = strings.ToLower(location) + "-" + v
		}

		result[apiv1.LabelZoneFailureDomain] = strings.Join(failureDomains[:], cloudvolume.LabelMultiZoneDelimiter)
	} else {
		result[apiv1.LabelZoneFailureDomain] = "0"
	}

	result[apiv1.LabelHostname] = nodeName
	return result
}

func buildNodeFromTemplate(scaleSetName, vmSizeName, location string, zones *[]string, windows bool, tags map[string]*string) (*apiv1.Node, error) {
	node := apiv1.Node{}
	nodeName := fmt.Sprintf("%s-asg-%d", scaleSetName, rand.Int63())

	node.ObjectMeta = metav1.ObjectMeta{
		Name:     nodeName,
		SelfLink: fmt.Sprintf("/api/v1/nodes/%s", nodeName),
		Labels:   map[string]string{},
	}

	node.Status = apiv1.NodeStatus{
		Capacity: apiv1.ResourceList{},
	}

	var vmssType *InstanceType
	for k := range InstanceTypes {
		if strings.EqualFold(k, vmSizeName) {
			vmssType = InstanceTypes[k]
			break
		}
	}

	promoRe := regexp.MustCompile(`(?i)_promo`)
	if promoRe.MatchString(vmSizeName) {
		if vmssType == nil {
			// We didn't find an exact match but this is a promo type, check for matching standard
			klog.V(1).Infof("No exact match found for %s, checking standard types", vmSizeName)
			skuName := promoRe.ReplaceAllString(vmSizeName, "")
			for k := range InstanceTypes {
				if strings.EqualFold(k, skuName) {
					vmssType = InstanceTypes[k]
					break
				}
			}
		}
	}

	if vmssType == nil {
		return nil, fmt.Errorf("instance type %q not supported", vmSizeName)
	}
	node.Status.Capacity[apiv1.ResourcePods] = *resource.NewQuantity(110, resource.DecimalSI)
	node.Status.Capacity[apiv1.ResourceCPU] = *resource.NewQuantity(vmssType.VCPU, resource.DecimalSI)
	node.Status.Capacity[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(vmssType.GPU, resource.DecimalSI)
	node.Status.Capacity[apiv1.ResourceMemory] = *resource.NewQuantity(vmssType.MemoryMb*1024*1024, resource.DecimalSI)

	resourcesFromTags := extractAllocatableResourcesFromScaleSet(tags)
	for resourceName, val := range resourcesFromTags {
		node.Status.Capacity[apiv1.ResourceName(resourceName)] = *val
	}

	// TODO: set real allocatable.
	node.Status.Allocatable = node.Status.Capacity

	// NodeLabels
	if tags != nil {
		for k, v := range tags {
			if v != nil {
				node.Labels[k] = *v
			} else {
				node.Labels[k] = ""
			}

		}
	}

	// GenericLabels
	node.Labels = cloudprovider.JoinStringMaps(node.Labels, buildGenericLabels(vmSizeName, nodeName, location, zones, windows))
	// Labels from the Scale Set's Tags
	node.Labels = cloudprovider.JoinStringMaps(node.Labels, extractLabelsFromScaleSet(tags))

	// Taints from the Scale Set's Tags
	node.Spec.Taints = extractTaintsFromScaleSet(tags)

	node.Status.Conditions = cloudprovider.BuildReadyConditions()
	return &node, nil
}

func extractLabelsFromScaleSet(tags map[string]*string) map[string]string {
	result := make(map[string]string)

	for tagName, tagValue := range tags {
		splits := strings.Split(tagName, nodeLabelTagName)
		if len(splits) > 1 {
			label := strings.Replace(splits[1], "_", "/", -1)
			if label != "" {
				result[label] = *tagValue
			}
		}
	}

	return result
}

func extractTaintsFromScaleSet(tags map[string]*string) []apiv1.Taint {
	taints := make([]apiv1.Taint, 0)

	for tagName, tagValue := range tags {
		// The tag value must be in the format <tag>:NoSchedule
		r, _ := regexp.Compile("(.*):(?:NoSchedule|NoExecute|PreferNoSchedule)")

		if r.MatchString(*tagValue) {
			splits := strings.Split(tagName, nodeTaintTagName)
			if len(splits) > 1 {
				values := strings.SplitN(*tagValue, ":", 2)
				if len(values) > 1 {
					taintKey := strings.Replace(splits[1], "_", "/", -1)
					taints = append(taints, apiv1.Taint{
						Key:    taintKey,
						Value:  values[0],
						Effect: apiv1.TaintEffect(values[1]),
					})
				}
			}
		}
	}

	return taints
}

func extractAllocatableResourcesFromScaleSet(tags map[string]*string) map[string]*resource.Quantity {
	resources := make(map[string]*resource.Quantity)

	for tagName, tagValue := range tags {
		resourceName := strings.Split(tagName, nodeResourcesTagName)
		if len(resourceName) < 2 || resourceName[1] == "" {
			continue
		}

		quantity, err := resource.ParseQuantity(*tagValue)
		if err != nil {
			continue
		}
		resources[resourceName[1]] = &quantity
	}

	return resources
}

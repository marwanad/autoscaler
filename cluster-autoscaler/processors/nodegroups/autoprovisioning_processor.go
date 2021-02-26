package nodegroups

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/labels"
	"k8s.io/klog/v2"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
)

// AutoprovisioningNodeGroupListProcessor adds autoprovisioning candidates to consider in scale-up.
type AutoprovisioningNodeGroupListProcessor struct {
}

// NewAutoprovisioningNodeGroupListProcessor creates an instance of NodeGroupListProcessor.
func NewAutoprovisioningNodeGroupListProcessor() NodeGroupListProcessor {
	return &AutoprovisioningNodeGroupListProcessor{}
}

// CleanUp cleans up the processor's internal structures.
func (p *AutoprovisioningNodeGroupListProcessor) CleanUp() {
}

// Process processes lists of unschedulable and sheduled pods before scaling of the cluster.
func (p *AutoprovisioningNodeGroupListProcessor) Process(context *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*schedulerframework.NodeInfo,
	unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*schedulerframework.NodeInfo, error) {

	if !context.AutoscalingOptions.NodeAutoprovisioningEnabled {
		return nodeGroups, nodeInfos, nil
	}

	autoprovisionedNodeGroupCount := 0
	for _, group := range nodeGroups {
		if group.Autoprovisioned() {
			autoprovisionedNodeGroupCount++
		}
	}
	if autoprovisionedNodeGroupCount >= context.MaxAutoprovisionedNodeGroupCount {
		klog.V(4).Infof("Max autoprovisioned node group count reached")
		return nodeGroups, nodeInfos, nil
	}

	newGroupsCount := 0

	newNodeGroups := addAllMachineTypesForConfig(context, map[string]string{}, map[string]resource.Quantity{},
		nodeInfos, unschedulablePods)
	newGroupsCount += len(newNodeGroups)
	nodeGroups = append(nodeGroups, newNodeGroups...)

	gpuRequests := GetGpuRequests(unschedulablePods)
	for _, gpuRequestInfo := range gpuRequests {
		klog.V(4).Info("Adding node groups using GPU to NAP simulations")
		extraResources := map[string]resource.Quantity{
			gpu.ResourceNvidiaGPU: gpuRequestInfo.MaxRequest,
		}
		newNodeGroups := addAllMachineTypesForConfig(context, gpuRequestInfo.SystemLabels, extraResources,
			nodeInfos, gpuRequestInfo.Pods)
		newGroupsCount += len(newNodeGroups)
		nodeGroups = append(nodeGroups, newNodeGroups...)
	}
	klog.V(4).Infof("Considering %v potential node groups in NAP simulations", newGroupsCount)

	return nodeGroups, nodeInfos, nil
}

func addAllMachineTypesForConfig(context *context.AutoscalingContext, systemLabels map[string]string, extraResources map[string]resource.Quantity,
	nodeInfos map[string]*schedulerframework.NodeInfo, unschedulablePods []*apiv1.Pod) []cloudprovider.NodeGroup {

	nodeGroups := make([]cloudprovider.NodeGroup, 0)
	machines, err := context.CloudProvider.GetAvailableMachineTypes()
	if err != nil {
		klog.Warningf("Failed to get machine types: %v", err)
		return nodeGroups
	}

	bestLabels := labels.BestLabelSet(unschedulablePods)
	taints := make([]apiv1.Taint, 0)
	for _, machineType := range machines {
		nodeGroup, err := context.CloudProvider.NewNodeGroup(machineType, bestLabels, systemLabels, taints, extraResources)
		if err != nil {
			// We don't check if a given node group setup is allowed.
			// It's fine if it isn't, just don't consider it an option.
			if err != cloudprovider.ErrIllegalConfiguration {
				klog.Warningf("Unable to build temporary node group for %s: %v", machineType, err)
			}
			continue
		}
		nodeInfo, err := nodeGroup.TemplateNodeInfo()
		if err != nil {
			klog.Warningf("Unable to build template for node group for %s: %v", nodeGroup.Id(), err)
			continue
		}
		nodeInfos[nodeGroup.Id()] = nodeInfo
		nodeGroups = append(nodeGroups, nodeGroup)
	}
	return nodeGroups
}

// GetGpuRequests returns a GpuRequestInfo for each type of GPU requested by
// any pod in pods argument. If the pod requests GPU, but doesn't specify what
// type of GPU it wants (via NodeSelector) it assumes it's DefaultGPUType.
func GetGpuRequests(pods []*apiv1.Pod) map[string]GpuRequestInfo {
	result := make(map[string]GpuRequestInfo)
	for _, pod := range pods {
		var podGpu resource.Quantity
		for _, container := range pod.Spec.Containers {
			if container.Resources.Requests != nil {
				containerGpu := container.Resources.Requests[ResourceNvidiaGPU]
				podGpu.Add(containerGpu)
			}
		}
		if podGpu.Value() == 0 {
			continue
		}

		gpuType := DefaultGPUType
		if gpuTypeFromSelector, found := pod.Spec.NodeSelector[GPULabel]; found {
			gpuType = gpuTypeFromSelector
		}

		requestInfo, found := result[gpuType]
		if !found {
			requestInfo = GpuRequestInfo{
				MaxRequest: podGpu,
				Pods:       make([]*apiv1.Pod, 0),
				SystemLabels: map[string]string{
					GPULabel: gpuType,
				},
			}
		}
		if podGpu.Cmp(requestInfo.MaxRequest) > 0 {
			requestInfo.MaxRequest = podGpu
		}
		requestInfo.Pods = append(requestInfo.Pods, pod)
		result[gpuType] = requestInfo
	}
	return result
}

// GpuRequestInfo contains an information about a set of pods requesting a GPU.
type GpuRequestInfo struct {
	// MaxRequest is maximum GPU request among pods
	MaxRequest resource.Quantity
	// Pods is a list of pods requesting GPU
	Pods []*apiv1.Pod
	// SystemLabels is a set of system labels corresponding to selected GPU
	// that needs to be passed to cloudprovider
	SystemLabels map[string]string
}

const (
	// ResourceNvidiaGPU is the name of the Nvidia GPU resource.
	ResourceNvidiaGPU = "nvidia.com/gpu"
	// GPULabel is the label added to nodes with GPU resource on Azure.
	GPULabel = "accelerator"
	// DefaultGPUType is the type of GPU used in NAP if the user
	// don't specify what type of GPU his pod wants.
	DefaultGPUType = "nvidia-tesla-k80"
)

package azure

import (
	"fmt"
	"k8s.io/klog"
	"k8s.io/klog/v2"
	"sync"
	"time"
)

type AzCacheHelper interface {
	// GetNodeGroupTargetSize returns targetSize for the node group
	GetNodeGroupTargetSize(ng AzureNodeGroup) (int64, error)
}

type azCacheHelper struct {
	mutex     sync.Mutex
	cache     *AzureCache
	client *azClient
	config   *Config

	sizeRefreshInterval time.Duration
	lastSizeRefresh   time.Time
}

func NewAzCacheHelper(cache *AzureCache, config *Config, azClient *azClient) AzCacheHelper {
	return &azCacheHelper{
		cache:     cache,
		config: config,
		client: azClient,
		sizeRefreshInterval: 15 * time.Second,
	}
}

// GetNodeGroupSize returns the node group size
func(az *azCacheHelper) GetNodeGroupTargetSize(ng AzureNodeGroup) (int64, error) {
	az.mutex.Lock()
	defer az.mutex.Unlock()

	targetSize, found := az.cache.GetNodeGroupTargetSize(ng.AzureRef())
	if az.lastSizeRefresh.Add(az.sizeRefreshInterval).After(time.Now()) && found {
		return targetSize, nil
	}

	// If not found or we require a cache refresh, we need to populate it
	if az.config.VMType == vmTypeVMSS {
		scaleSet, _ := ng.(*ScaleSet)
		targetSizes, err := az.populateTargetSizesForScaleSets(scaleSet)
		if err != nil {
			if found {
				klog.Warningf("got error while attempting to populate scale set target sizes %s. Returning cached target size for node group %q.", err, ng.AzureRef().Name)
				return targetSize, nil
			}
			return -1, err
		}

		size, found := targetSizes[ng.AzureRef()]
		if !found {
			return -1, fmt.Errorf("could not get target size for node group: %q", ng.AzureRef())
		}

		return size, nil
	}
	return -1, fmt.Errorf("could not get target size for node group: %q", ng.AzureRef())
}


func(az *azCacheHelper) populateTargetSizesForScaleSets(scaleSet *ScaleSet) (map[AzureRef]int64, error) {
	newTargetSizeCache := map[AzureRef]int64{}

	allVMSS, rerr := scaleSet.GetAllVMSSInfo()
	if rerr != nil {
		return nil, rerr.Error()
	}

	registeredNodeGroups := az.getNodeGroupRefs()
	for _, vmss := range allVMSS {
		azRef := AzureRef{
			Name: *vmss.Name,
		}
		if registeredNodeGroups[azRef] {
			newTargetSizeCache[azRef] = *vmss.Sku.Capacity
		}
	}

	for migRef, targetSize := range newTargetSizeCache {
		az.cache.SetNodeGroupTargetSize(migRef, targetSize)
	}
	az.lastSizeRefresh = time.Now()
	return newTargetSizeCache, nil

}


func (az *azCacheHelper) getNodeGroupRefs() map[AzureRef]bool {
	nodeGroupRefs := make(map[AzureRef]bool)
	for _, ng := range az.cache.GetNodeGroups() {
		nodeGroupRefs[ng.AzureRef()] = true
	}
	return nodeGroupRefs
}

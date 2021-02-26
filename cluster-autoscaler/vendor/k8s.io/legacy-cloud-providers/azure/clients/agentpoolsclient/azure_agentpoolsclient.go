// +build !providerless

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

package agentpoolsclient

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/services/containerservice/mgmt/2020-04-01/containerservice"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/to"

	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"
	azclients "k8s.io/legacy-cloud-providers/azure/clients"
	"k8s.io/legacy-cloud-providers/azure/clients/armclient"
	"k8s.io/legacy-cloud-providers/azure/metrics"
	"k8s.io/legacy-cloud-providers/azure/retry"
)

var _ Interface = &Client{}

// Client implements agentpools client Interface.
type Client struct {
	armClient      armclient.Interface
	subscriptionID string

	// Rate limiting configures.
	rateLimiterReader flowcontrol.RateLimiter
	rateLimiterWriter flowcontrol.RateLimiter

	// ARM throttling configures.
	RetryAfterReader time.Time
	RetryAfterWriter time.Time
}

// New creates a new ContainerServiceClient client with ratelimiting.
func New(config *azclients.ClientConfig) *Client {
	baseURI := config.ResourceManagerEndpoint
	authorizer := config.Authorizer
	armClient := armclient.New(authorizer, baseURI, config.UserAgent, APIVersion, config.Location, config.Backoff)
	rateLimiterReader, rateLimiterWriter := azclients.NewRateLimiter(config.RateLimitConfig)

	klog.V(2).Infof("Azure ContainerServiceClient (read ops) using rate limit config: QPS=%g, bucket=%d",
		config.RateLimitConfig.CloudProviderRateLimitQPS,
		config.RateLimitConfig.CloudProviderRateLimitBucket)
	klog.V(2).Infof("Azure ContainerServiceClient (write ops) using rate limit config: QPS=%g, bucket=%d",
		config.RateLimitConfig.CloudProviderRateLimitQPSWrite,
		config.RateLimitConfig.CloudProviderRateLimitBucketWrite)

	client := &Client{
		armClient:         armClient,
		rateLimiterReader: rateLimiterReader,
		rateLimiterWriter: rateLimiterWriter,
		subscriptionID:    config.SubscriptionID,
	}

	return client
}

// Get gets a AgentPool.
func (c *Client) Get(ctx context.Context, resourceGroupName, managedClusterName, agentPoolName string) (containerservice.AgentPool, *retry.Error) {
	mc := metrics.NewMetricContext("agent_pools", "get", resourceGroupName, c.subscriptionID, "")

	// Report errors if the client is rate limited.
	if !c.rateLimiterReader.TryAccept() {
		mc.RateLimitedCount()
		return containerservice.AgentPool{}, retry.GetRateLimitError(false, "GetAgentPool")
	}

	// Report errors if the client is throttled.
	if c.RetryAfterReader.After(time.Now()) {
		mc.ThrottledCount()
		rerr := retry.GetThrottlingError("GetAgentPool", "client throttled", c.RetryAfterReader)
		return containerservice.AgentPool{}, rerr
	}

	result, rerr := c.getAgentPool(ctx, resourceGroupName, managedClusterName, agentPoolName)
	mc.Observe(rerr.Error())
	if rerr != nil {
		if rerr.IsThrottled() {
			// Update RetryAfterReader so that no more requests would be sent until RetryAfter expires.
			c.RetryAfterReader = rerr.RetryAfter
		}

		return result, rerr
	}

	return result, nil
}

// getAgentPool gets a AgentPool.
func (c *Client) getAgentPool(ctx context.Context, resourceGroupName, managedClusterName, agentPoolName string) (containerservice.AgentPool, *retry.Error) {
	resourceID := armclient.GetResourceID(
		c.subscriptionID,
		resourceGroupName,
		"Microsoft.ContainerService/managedClusters",
		fmt.Sprintf("%s/agentpools/%s", managedClusterName, agentPoolName),
	)
	result := containerservice.AgentPool{}

	response, rerr := c.armClient.GetResource(ctx, resourceID, "")
	defer c.armClient.CloseResponse(ctx, response)
	if rerr != nil {
		klog.V(5).Infof("Received error in %s: resourceID: %s, error: %s", "agentpool.get.request", resourceID, rerr.Error())
		return result, rerr
	}

	err := autorest.Respond(
		response,
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result))
	if err != nil {
		klog.V(5).Infof("Received error in %s: resourceID: %s, error: %s", "agentpool.get.respond", resourceID, err)
		return result, retry.GetError(response, err)
	}

	result.Response = autorest.Response{Response: response}
	return result, nil
}

// List gets a list of AgentPool in a cluster.
func (c *Client) List(ctx context.Context, resourceGroupName, managedClusterName string) ([]containerservice.AgentPool, *retry.Error) {
	mc := metrics.NewMetricContext("managed_clusters", "list", resourceGroupName, c.subscriptionID, "")

	// Report errors if the client is rate limited.
	if !c.rateLimiterReader.TryAccept() {
		mc.RateLimitedCount()
		return nil, retry.GetRateLimitError(false, "ListAgentPools")
	}

	// Report errors if the client is throttled.
	if c.RetryAfterReader.After(time.Now()) {
		mc.ThrottledCount()
		rerr := retry.GetThrottlingError("ListAgentPools", "client throttled", c.RetryAfterReader)
		return nil, rerr
	}

	result, rerr := c.listAgentPools(ctx, resourceGroupName, managedClusterName)
	mc.Observe(rerr.Error())
	if rerr != nil {
		if rerr.IsThrottled() {
			// Update RetryAfterReader so that no more requests would be sent until RetryAfter expires.
			c.RetryAfterReader = rerr.RetryAfter
		}

		return result, rerr
	}

	return result, nil
}

// listAgentPool gets a list of AgentPools in the resource group.
func (c *Client) listAgentPools(ctx context.Context, resourceGroupName, managedClusterName string) ([]containerservice.AgentPool, *retry.Error) {
	resourceID := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.ContainerService/managedClusters/%s/agentPools",
		autorest.Encode("path", c.subscriptionID),
		autorest.Encode("path", resourceGroupName),
		autorest.Encode("path", managedClusterName),
	)
	result := make([]containerservice.AgentPool, 0)
	page := &AgentPoolResultPage{}
	page.fn = c.listNextResults

	resp, rerr := c.armClient.GetResource(ctx, resourceID, "")
	defer c.armClient.CloseResponse(ctx, resp)
	if rerr != nil {
		klog.V(5).Infof("Received error in %s: resourceID: %s, error: %s", "agentpool.list.request", resourceID, rerr.Error())
		return result, rerr
	}

	var err error
	page.mclr, err = c.listResponder(resp)
	if err != nil {
		klog.V(5).Infof("Received error in %s: resourceID: %s, error: %s", "agentpool.list.respond", resourceID, err)
		return result, retry.GetError(resp, err)
	}

	for {
		result = append(result, page.Values()...)

		// Abort the loop when there's no nextLink in the response.
		if to.String(page.Response().NextLink) == "" {
			break
		}

		if err = page.NextWithContext(ctx); err != nil {
			klog.V(5).Infof("Received error in %s: resourceID: %s, error: %s", "agentpool.list.next", resourceID, err)
			return result, retry.GetError(page.Response().Response.Response, err)
		}
	}

	return result, nil
}

func (c *Client) listResponder(resp *http.Response) (result containerservice.AgentPoolListResult, err error) {
	err = autorest.Respond(
		resp,
		autorest.ByIgnoring(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result))
	result.Response = autorest.Response{Response: resp}
	return
}

// managedClusterListResultPreparer prepares a request to retrieve the next set of results.
// It returns nil if no more results exist.
func (c *Client) managedClusterListResultPreparer(ctx context.Context, mclr containerservice.AgentPoolListResult) (*http.Request, error) {
	if mclr.NextLink == nil || len(to.String(mclr.NextLink)) < 1 {
		return nil, nil
	}

	decorators := []autorest.PrepareDecorator{
		autorest.WithBaseURL(to.String(mclr.NextLink)),
	}
	return c.armClient.PrepareGetRequest(ctx, decorators...)
}

// listNextResults retrieves the next set of results, if any.
func (c *Client) listNextResults(ctx context.Context, lastResults containerservice.AgentPoolListResult) (result containerservice.AgentPoolListResult, err error) {
	req, err := c.managedClusterListResultPreparer(ctx, lastResults)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "agentpoolclient", "listNextResults", nil, "Failure preparing next results request")
	}
	if req == nil {
		return
	}

	resp, rerr := c.armClient.Send(ctx, req)
	defer c.armClient.CloseResponse(ctx, resp)
	if rerr != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(rerr.Error(), "agentpoolclient", "listNextResults", resp, "Failure sending next results request")
	}

	result, err = c.listResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "agentpoolclient", "listNextResults", resp, "Failure responding to next results request")
	}

	return
}

// AgentPoolResultPage contains a page of AgentPool values.
type AgentPoolResultPage struct {
	fn   func(context.Context, containerservice.AgentPoolListResult) (containerservice.AgentPoolListResult, error)
	mclr containerservice.AgentPoolListResult
}

// NextWithContext advances to the next page of values.  If there was an error making
// the request the page does not advance and the error is returned.
func (page *AgentPoolResultPage) NextWithContext(ctx context.Context) (err error) {
	next, err := page.fn(ctx, page.mclr)
	if err != nil {
		return err
	}
	page.mclr = next
	return nil
}

// Next advances to the next page of values.  If there was an error making
// the request the page does not advance and the error is returned.
// Deprecated: Use NextWithContext() instead.
func (page *AgentPoolResultPage) Next() error {
	return page.NextWithContext(context.Background())
}

// NotDone returns true if the page enumeration should be started or is not yet complete.
func (page AgentPoolResultPage) NotDone() bool {
	return !page.mclr.IsEmpty()
}

// Response returns the raw server response from the last page request.
func (page AgentPoolResultPage) Response() containerservice.AgentPoolListResult {
	return page.mclr
}

// Values returns the slice of values for the current page or nil if there are no values.
func (page AgentPoolResultPage) Values() []containerservice.AgentPool {
	if page.mclr.IsEmpty() {
		return nil
	}
	return *page.mclr.Value
}

// CreateOrUpdate creates or updates an agetpool asynchronously
func (c *Client) CreateOrUpdateAsync(ctx context.Context, resourceGroupName, managedClusterName, agentPoolName string, parameters containerservice.AgentPool, etag string) (*azure.Future, *retry.Error) {
	mc := metrics.NewMetricContext("managed_clusters", "create_or_update", resourceGroupName, c.subscriptionID, "")

	// Report errors if the client is rate limited.
	if !c.rateLimiterWriter.TryAccept() {
		mc.RateLimitedCount()
		return nil, retry.GetRateLimitError(true, "CreateOrUpdateAgentPool")
	}

	// Report errors if the client is throttled.
	if c.RetryAfterWriter.After(time.Now()) {
		mc.ThrottledCount()
		rerr := retry.GetThrottlingError("CreateOrUpdateAgentPool", "client throttled", c.RetryAfterWriter)
		return nil, rerr
	}

	resourceID := armclient.GetResourceID(
		c.subscriptionID,
		resourceGroupName,
		"Microsoft.ContainerService/managedClusters",
		fmt.Sprintf("%s/agentpools/%s", managedClusterName, agentPoolName),
	)
	decorators := []autorest.PrepareDecorator{
		autorest.WithPathParameters("{resourceID}", map[string]interface{}{"resourceID": resourceID}),
		autorest.WithJSON(parameters),
	}
	if etag != "" {
		decorators = append(decorators, autorest.WithHeader("If-Match", autorest.String(etag)))
	}

	future, rerr := c.armClient.PutResourceWithDecoratorsAsync(ctx, resourceID, parameters, decorators)
	mc.Observe(rerr.Error())
	if rerr != nil {
		if rerr.IsThrottled() {
			// Update RetryAfterReader so that no more requests would be sent until RetryAfter expires.
			c.RetryAfterWriter = rerr.RetryAfter
		}

		return nil, rerr
	}

	return future, nil
}

func (c *Client) WaitForCreateOrUpdateResult(ctx context.Context, future *azure.Future, resourceGroupName string) (*http.Response, error) {
	mc := metrics.NewMetricContext("managed_clusters", "wait_for_create_or_update_result", resourceGroupName, c.subscriptionID, "")
	res, err := c.armClient.WaitForAsyncOperationResult(ctx, future, "AgentPoolWaitForCreateOrUpdateResult")
	mc.Observe(err)
	return res, err
}


// CreateOrUpdate creates or updates a AgentPool.
func (c *Client) CreateOrUpdate(ctx context.Context, resourceGroupName, managedClusterName, agentPoolName string, parameters containerservice.AgentPool, etag string) *retry.Error {
	mc := metrics.NewMetricContext("managed_clusters", "create_or_update", resourceGroupName, c.subscriptionID, "")

	// Report errors if the client is rate limited.
	if !c.rateLimiterWriter.TryAccept() {
		mc.RateLimitedCount()
		return retry.GetRateLimitError(true, "CreateOrUpdateAgentPool")
	}

	// Report errors if the client is throttled.
	if c.RetryAfterWriter.After(time.Now()) {
		mc.ThrottledCount()
		rerr := retry.GetThrottlingError("CreateOrUpdateAgentPool", "client throttled", c.RetryAfterWriter)
		return rerr
	}

	rerr := c.createOrUpdateAgentPool(ctx, resourceGroupName, managedClusterName, agentPoolName, parameters, etag)
	mc.Observe(rerr.Error())
	if rerr != nil {
		if rerr.IsThrottled() {
			// Update RetryAfterReader so that no more requests would be sent until RetryAfter expires.
			c.RetryAfterWriter = rerr.RetryAfter
		}

		return rerr
	}

	return nil
}

// createOrUpdateAgentPool creates or updates a AgentPool.
func (c *Client) createOrUpdateAgentPool(ctx context.Context, resourceGroupName, managedClusterName, agentPoolName string, parameters containerservice.AgentPool, etag string) *retry.Error {
	resourceID := armclient.GetResourceID(
		c.subscriptionID,
		resourceGroupName,
		"Microsoft.ContainerService/managedClusters",
		fmt.Sprintf("%s/agentpools/%s", managedClusterName, agentPoolName),
	)
	decorators := []autorest.PrepareDecorator{
		autorest.WithPathParameters("{resourceID}", map[string]interface{}{"resourceID": resourceID}),
		autorest.WithJSON(parameters),
	}
	if etag != "" {
		decorators = append(decorators, autorest.WithHeader("If-Match", autorest.String(etag)))
	}

	response, rerr := c.armClient.PutResourceWithDecorators(ctx, resourceID, parameters, decorators)
	defer c.armClient.CloseResponse(ctx, response)
	if rerr != nil {
		klog.V(5).Infof("Received error in %s: resourceID: %s, error: %s", "managedCluster.put.request", resourceID, rerr.Error())
		return rerr
	}

	if response != nil && response.StatusCode != http.StatusNoContent {
		_, rerr = c.createOrUpdateResponder(response)
		if rerr != nil {
			klog.V(5).Infof("Received error in %s: resourceID: %s, error: %s", "managedCluster.put.respond", resourceID, rerr.Error())
			return rerr
		}
	}

	return nil
}

func (c *Client) createOrUpdateResponder(resp *http.Response) (*containerservice.AgentPool, *retry.Error) {
	result := &containerservice.AgentPool{}
	err := autorest.Respond(
		resp,
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusCreated),
		autorest.ByUnmarshallingJSON(&result))
	result.Response = autorest.Response{Response: resp}
	return result, retry.GetError(resp, err)
}

// Delete deletes a AgentPool by name.
func (c *Client) Delete(ctx context.Context, resourceGroupName, managedClusterName, agentPoolName string) *retry.Error {
	mc := metrics.NewMetricContext("managed_clusters", "delete", resourceGroupName, c.subscriptionID, "")

	// Report errors if the client is rate limited.
	if !c.rateLimiterWriter.TryAccept() {
		mc.RateLimitedCount()
		return retry.GetRateLimitError(true, "DeleteAgentPool")
	}

	// Report errors if the client is throttled.
	if c.RetryAfterWriter.After(time.Now()) {
		mc.ThrottledCount()
		rerr := retry.GetThrottlingError("DeleteAgentPool", "client throttled", c.RetryAfterWriter)
		return rerr
	}

	rerr := c.deleteAgentPool(ctx, resourceGroupName, managedClusterName, agentPoolName)
	mc.Observe(rerr.Error())
	if rerr != nil {
		if rerr.IsThrottled() {
			// Update RetryAfterReader so that no more requests would be sent until RetryAfter expires.
			c.RetryAfterWriter = rerr.RetryAfter
		}

		return rerr
	}

	return nil
}

// deleteAgentPool deletes a AgentPool by name.
func (c *Client) deleteAgentPool(ctx context.Context, resourceGroupName, managedClusterName, agentPoolName string) *retry.Error {
	resourceID := armclient.GetResourceID(
		c.subscriptionID,
		resourceGroupName,
		"Microsoft.ContainerService/managedClusters",
		fmt.Sprintf("%s/agentpools/%s", managedClusterName, agentPoolName),
	)

	return c.armClient.DeleteResource(ctx, resourceID, "")
}

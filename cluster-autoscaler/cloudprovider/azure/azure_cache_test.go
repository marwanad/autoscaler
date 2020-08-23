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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegister(t *testing.T) {
	provider := newTestProvider(t)
	ss := newTestScaleSet(provider.azureManager, "ss")

	ac := NewAzureCache()
	ac.registeredNodeGroups[azureRef{Name: "ss"}] = ss

	isSuccess := ac.Register(ss)
	assert.False(t, isSuccess)

	ss1 := newTestScaleSet(provider.azureManager, "ss")
	ss1.minSize = 2
	isSuccess = ac.Register(ss1)
	assert.True(t, isSuccess)
}

func TestUnRegister(t *testing.T) {
	provider := newTestProvider(t)
	ss := newTestScaleSet(provider.azureManager, "ss")
	ss1 := newTestScaleSet(provider.azureManager, "ss1")

	ac := NewAzureCache()
	ac.registeredNodeGroups[azureRef{Name: "ss"}] = ss
	ac.registeredNodeGroups[azureRef{Name: "ss1"}] = ss1

	isSuccess := ac.Unregister(ss)
	assert.True(t, isSuccess)
	assert.Equal(t, 1, len(ac.registeredNodeGroups))
}

func TestFindForInstance(t *testing.T) {
	ac := NewAzureCache()

	inst := azureRef{Name: "/subscriptions/sub/resourceGroups/rg/providers/foo"}
	ac.notInRegisteredNodeGroups = make(map[azureRef]struct{})
	ac.notInRegisteredNodeGroups[inst] = struct{}{}
	nodeGroup, err := ac.GetNodeGroupForInstance(&inst, vmTypeVMSS)
	assert.Nil(t, nodeGroup)
	assert.NoError(t, err)

	delete(ac.notInRegisteredNodeGroups, inst)
	nodeGroup, err = ac.GetNodeGroupForInstance(&inst, vmTypeStandard)
	assert.Nil(t, nodeGroup)
	assert.NoError(t, err)
	_, found := ac.notInRegisteredNodeGroups[inst]
	assert.False(t, found)
}

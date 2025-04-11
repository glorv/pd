// Copyright 2024 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package keypath

import (
	"fmt"
	"strconv"
	"strings"
)

// ControllerConfigPath returns the path to save the controller config.
func ControllerConfigPath() string {
	return controllerConfigPath
}

func keyspaceResourceGroupName(keyspaceID uint32, groupName string) string {
	kGroupName := groupName
	// keep it compatible with old format by directly reusing the group name if its the default keyspace
	if keyspaceID != 0 {
		kGroupName = fmt.Sprintf("%d/%s", keyspaceID, groupName)
	}
	return kGroupName
}

func ParseKeyspaceGroupName(key string) (uint32, string) {
	segments := strings.Split(key, "/")
	if len(segments) == 1 {
		return 0, key
	}
	id, _ := strconv.Atoi(segments[0])
	return uint32(id), segments[1]
}

// ResourceGroupSettingPath returns the path to save the resource group settings.
func ResourceGroupSettingPath(keyspaceID uint32, groupName string) string {
	kGroupName := keyspaceResourceGroupName(keyspaceID, groupName)
	return fmt.Sprintf(resourceGroupSettingsPathFormat, kGroupName)
}

// ResourceGroupStatePath returns the path to save the resource group states.
func ResourceGroupStatePath(keyspaceID uint32, groupName string) string {
	kGroupName := keyspaceResourceGroupName(keyspaceID, groupName)
	return fmt.Sprintf(resourceGroupStatesPathFormat, kGroupName)
}

// ResourceGroupSettingPrefix returns the prefix of the resource group settings.
func ResourceGroupSettingPrefix() string {
	return ResourceGroupSettingPath(0, "")
}

// ResourceGroupStatePrefix returns the prefix of the resource group states.
func ResourceGroupStatePrefix() string {
	return ResourceGroupStatePath(0, "")
}

func KeyspaceSettingPath(keyspaceID uint32) string {
	return fmt.Sprintf(keyspaceSettingsPathFormat, keyspaceID)
}

func KeyspaceSettingPrefix() string {
	return "resource_manager/keyspace/"
}

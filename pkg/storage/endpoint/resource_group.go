// Copyright 2022 TiKV Project Authors.
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

package endpoint

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/tikv/pd/pkg/utils/keypath"
)

// ResourceGroupStorage defines the storage operations on the resource group.
type ResourceGroupStorage interface {
	LoadResourceGroupSettings(f func(keyspaceID uint32, name string, v string)) error
	SaveResourceGroupSetting(keyspaceID uint32, name string, msg proto.Message) error
	DeleteResourceGroupSetting(keyspaceID uint32, name string) error
	LoadResourceGroupStates(f func(keyspaceID uint32, name string, v string)) error
	SaveResourceGroupStates(keyspaceID uint32, name string, obj any) error
	DeleteResourceGroupStates(keyspaceID uint32, name string) error
	SaveControllerConfig(config any) error
	LoadControllerConfig() (string, error)
	LoadKeyspaceSettings(f func(id, v string)) error
	SaveKeyspaceSetting(keyspaceID uint32, obj any) error
}

var _ ResourceGroupStorage = (*StorageEndpoint)(nil)

// SaveResourceGroupSetting stores a resource group to storage.
func (se *StorageEndpoint) SaveResourceGroupSetting(keyspaceID uint32, name string, msg proto.Message) error {
	return se.saveProto(keypath.ResourceGroupSettingPath(keyspaceID, name), msg)
}

// DeleteResourceGroupSetting removes a resource group from storage.
func (se *StorageEndpoint) DeleteResourceGroupSetting(keyspaceID uint32, name string) error {
	return se.Remove(keypath.ResourceGroupSettingPath(keyspaceID, name))
}

// LoadResourceGroupSettings loads all resource groups from storage.
func (se *StorageEndpoint) LoadResourceGroupSettings(f func(keyspaceID uint32, name string, v string)) error {
	return se.loadRangeByPrefix(keypath.ResourceGroupSettingPrefix(), func(k, v string) {
		keyspaceID, name := keypath.ParseKeyspaceGroupName(k)
		f(keyspaceID, name, v)
	})
}

// SaveResourceGroupStates stores a resource group to storage.
func (se *StorageEndpoint) SaveResourceGroupStates(keyspaceID uint32, name string, obj any) error {
	return se.saveJSON(keypath.ResourceGroupStatePath(keyspaceID, name), obj)
}

// DeleteResourceGroupStates removes a resource group from storage.
func (se *StorageEndpoint) DeleteResourceGroupStates(keyspaceID uint32, name string) error {
	return se.Remove(keypath.ResourceGroupStatePath(keyspaceID, name))
}

// LoadResourceGroupStates loads all resource groups from storage.
func (se *StorageEndpoint) LoadResourceGroupStates(f func(keyspaceID uint32, name, v string)) error {
	return se.loadRangeByPrefix(keypath.ResourceGroupStatePrefix(), func(k, v string) {
		keyspaceID, name := keypath.ParseKeyspaceGroupName(k)
		f(keyspaceID, name, v)
	})
}

// SaveControllerConfig stores the resource controller config to storage.
func (se *StorageEndpoint) SaveControllerConfig(config any) error {
	return se.saveJSON(keypath.ControllerConfigPath(), config)
}

// LoadControllerConfig loads the resource controller config from storage.
func (se *StorageEndpoint) LoadControllerConfig() (string, error) {
	return se.Load(keypath.ControllerConfigPath())
}

func (se *StorageEndpoint) LoadKeyspaceSettings(f func(id, v string)) error {
	log.Info("load keyspace settings", zap.String("prefix", keypath.KeyspaceSettingPrefix()))
	return se.loadRangeByPrefix(keypath.KeyspaceSettingPrefix(), func(k, v string) {
		f(k, v)
	})
}

func (se *StorageEndpoint) SaveKeyspaceSetting(keyspaceID uint32, obj any) error {
	return se.saveJSON(keypath.KeyspaceSettingPath(keyspaceID), obj)
}

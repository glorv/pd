package server

import (
	"sort"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.uber.org/zap"
)

const groupSlowExpiredDuration = 60 * time.Second

type ResourceGroupTokenTracker struct {
	ruPerSec           float64
	overrideRUPerSec   float64
	consumeTokenWindow SlideWindow
}

type KeyspaceConfig struct {
	ID      uint32  `json:"id"`
	Name    string  `json:"name"`
	RULimit float64 `json:"ru_limit"`
}

// ResourceGroup is the definition of a resource group, for REST API.
type KeyspaceResourceGroupManager struct {
	syncutil.RWMutex
	KeyspaceConfig
	// resource_group_name --> resource_group
	groups map[string]*ResourceGroup
	// RU token consumption tracker
	// group_name --> token_history
	groupTokens map[string]*ResourceGroupTokenTracker

	storage endpoint.ResourceGroupStorage
}

func NewKeyspaceResourceGroupManager(
	ks KeyspaceConfig,
	storage endpoint.ResourceGroupStorage,
) *KeyspaceResourceGroupManager {
	return &KeyspaceResourceGroupManager{
		KeyspaceConfig: ks,
		groups:         make(map[string]*ResourceGroup),
		groupTokens:    make(map[string]*ResourceGroupTokenTracker),
		storage:        storage,
	}
}

func (km *KeyspaceResourceGroupManager) AddResourceGroup(grouppb *rmpb.ResourceGroup) error {
	// Check the name.
	if len(grouppb.Name) == 0 || len(grouppb.Name) > 32 {
		return errs.ErrInvalidGroup
	}
	// Check the Priority.
	if grouppb.GetPriority() > 16 {
		return errs.ErrInvalidGroup
	}
	group := FromProtoResourceGroup(grouppb)
	km.Lock()
	defer km.Unlock()
	if err := group.persistSettings(km.storage); err != nil {
		return err
	}
	if err := group.persistStates(km.storage); err != nil {
		return err
	}
	km.groups[grouppb.Name] = group
	return nil
}

// ModifyResourceGroup modifies an existing resource group.
func (km *KeyspaceResourceGroupManager) ModifyResourceGroup(group *rmpb.ResourceGroup) error {
	if group == nil || group.Name == "" {
		return errs.ErrInvalidGroup
	}
	km.Lock()
	curGroup, ok := km.groups[group.Name]
	if !ok {
		return errs.ErrResourceGroupNotExists.FastGenByArgs(group.Name)
	}

	err := curGroup.PatchSettings(group)
	if err != nil {
		return err
	}
	if tracker, ok := km.groupTokens[group.Name]; ok {
		tracker.ruPerSec = float64(group.GetRUSettings().GetRU().GetSettings().FillRate)
	}
	km.Unlock()
	return curGroup.persistSettings(km.storage)
}

// DeleteResourceGroup deletes a resource group.
func (km *KeyspaceResourceGroupManager) DeleteResourceGroup(name string) error {
	if name == reservedDefaultGroupName {
		return errs.ErrDeleteReservedGroup
	}
	if err := km.storage.DeleteResourceGroupSetting(km.ID, name); err != nil {
		return err
	}

	km.Lock()
	delete(km.groups, name)
	delete(km.groupTokens, name)
	km.Unlock()
	return nil
}

// GetResourceGroup returns a copy of a resource group.
func (km *KeyspaceResourceGroupManager) GetResourceGroup(name string, withStats bool) *ResourceGroup {
	km.RLock()
	defer km.RUnlock()
	if group, ok := km.groups[name]; ok {
		return group.Clone(withStats)
	}
	return nil
}

// GetMutableResourceGroup returns a mutable resource group.
func (km *KeyspaceResourceGroupManager) GetMutableResourceGroup(name string) *ResourceGroup {
	km.RLock()
	defer km.RUnlock()
	if group, ok := km.groups[name]; ok {
		return group
	}
	return nil
}

// GetResourceGroupList returns copies of resource group list.
func (km *KeyspaceResourceGroupManager) GetResourceGroupList(withStats bool) []*ResourceGroup {
	km.RLock()
	res := make([]*ResourceGroup, 0, len(km.groups))
	for _, group := range km.groups {
		res = append(res, group.Clone(withStats))
	}
	km.RUnlock()
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name < res[j].Name
	})
	return res
}

func (km *KeyspaceResourceGroupManager) ReportConsumption(c *RUConsumptionRecord) {
	km.Lock()
	defer km.Unlock()

	group, ok := km.groups[c.resourceGroupName]
	if !ok {
		return
	}

	if c.isBackground || c.isTiFlash {
		return
	}

	tracker, ok := km.groupTokens[c.resourceGroupName]
	if !ok {
		fillRate := group.getFillRate()
		tracker = &ResourceGroupTokenTracker{
			ruPerSec: fillRate,
			overrideRUPerSec: fillRate,
			consumeTokenWindow: SlideWindow{
				sampleDuration: 10 * time.Second,
			},
		}
		km.groupTokens[c.resourceGroupName] = tracker
	}
	tracker.consumeTokenWindow.Observe(c.RRU + c.WRU)
}

// persistStates persists the resource group tokens.
func (km *KeyspaceResourceGroupManager) persistStates() {
	km.RLock()
	defer km.RUnlock()
	for _, rg := range km.groups {
		if err := rg.persistStates(km.storage); err != nil {
			log.Error("persist resource group state failed", zap.Error(err))
		}
	}
}

func (km *KeyspaceResourceGroupManager) updateResourceGroupRULimits() {
	km.Lock()
	defer km.Unlock()

	totalLatestTokens := 0.
	totalAvgTokens := 0.
	totalRU := 0.0
	totalActiveRU := 0.
	idleGroups := make(map[string]struct{}, 0)
	now := time.Now()
	for name, track := range km.groupTokens {
		if now.Sub(track.consumeTokenWindow.currentSampleStart) >= groupSlowExpiredDuration {
			totalRU += track.ruPerSec
			idleGroups[name] = struct{}{}
			continue
		}
		totalLatestTokens += track.consumeTokenWindow.LatestSamplePerSec()
		totalAvgTokens += track.consumeTokenWindow.LatestSamplePerSec()
		totalActiveRU += track.ruPerSec
	}

	idleThreshold := km.RULimit / 2
	if totalLatestTokens < idleThreshold && totalAvgTokens < idleThreshold {
		for name := range km.groupTokens {
			group, ok := km.groups[name]
			if !ok {
				continue
			}
			fillRate := group.getFillRate()
			if fillRate > km.RULimit {
				fillRate = km.RULimit
			}
			if t, ok := km.groupTokens[name]; ok && t.overrideRUPerSec != fillRate {
				t.overrideRUPerSec = fillRate
				group.SetOverrideFillRate(fillRate)
			}
		}

		return
	}

	if totalLatestTokens > km.RULimit * 0.8 {
		for name, track := range km.groupTokens {
			if now.Sub(track.consumeTokenWindow.currentSampleStart) >= groupSlowExpiredDuration {
				continue
			}
			group, ok := km.groups[name]
			if !ok {
				continue
			}
			newRate := km.RULimit * track.ruPerSec / totalActiveRU
			if newRate / track.ruPerSec >= 0.98 && newRate / track.ruPerSec <= 1.02 {
				newRate = track.ruPerSec
			}
			if  newRate != track.overrideRUPerSec {
				track.overrideRUPerSec = newRate
				group.SetOverrideFillRate(newRate)
			}	
		}
		return
	}

	for name, track := range km.groupTokens {
		if now.Sub(track.consumeTokenWindow.currentSampleStart) >= groupSlowExpiredDuration {
			continue
		}
		group, ok := km.groups[name]
		if !ok {
			continue
		}
		newRate := km.RULimit * track.ruPerSec / totalActiveRU
		if newRate / track.ruPerSec >= 0.98 && newRate / track.ruPerSec <= 1.02 {
			newRate = track.ruPerSec
		}
		if  newRate > track.overrideRUPerSec {
			track.overrideRUPerSec = newRate
			group.SetOverrideFillRate(newRate)
		}	
	}
}

const SAMPLE_COUNT = 5

type SlideWindow struct {
	samples            [SAMPLE_COUNT]float64
	lastIndex          int
	currentSample      float64
	currentSampleStart time.Time
	sampleDuration     time.Duration
}

func (w *SlideWindow) Observe(v float64) {
	now := time.Now()
	// try to fill missing samples with latest sample.
	dur := now.Sub(w.currentSampleStart)
	if dur > w.sampleDuration {
		sampleCount := dur / w.sampleDuration
		for i := 0; i <= int(sampleCount); i++ {
			w.lastIndex = (w.lastIndex + 1) % 5
			w.samples[w.lastIndex] = w.currentSample
		}
		w.currentSample = 0.
		w.currentSampleStart = w.currentSampleStart.Add(w.sampleDuration * sampleCount)
	}
	w.currentSample += v
}

func (w *SlideWindow) LatestSamplePerSec() float64 {
	return w.samples[w.lastIndex] / float64(int(w.sampleDuration/time.Second))
}

func (w *SlideWindow) AvgSamplePerSec() float64 {
	sum := 0.
	for _, s := range w.samples {
		sum += s
	}

	return sum / float64(int(w.sampleDuration/time.Second)*len(w.samples))
}

func (w *SlideWindow) LatestSampleTime() time.Time {
	return w.currentSampleStart
}

package server

import (
	"fmt"
	"sort"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const groupSlowExpiredDuration = 30 * time.Second

type ResourceGroupTokenTracker struct {
	fillRate float64
	// 0 for high, 1 for medium, 2 for low.
	priority           int
	overrideRUPerSec   float64
	consumeTokenWindow SlideWindow
}

func (rt *ResourceGroupTokenTracker) SetGroupConfig(tokenFillRate float64, priority uint32) {
	rt.fillRate = tokenFillRate
	rt.priority = groupPriority2TrackerPriority(priority)
}

func groupPriority2TrackerPriority(priority uint32) int {
	// mapping resource group priority to 0~2.
	// low(1~5) -> 2
	// medium(6~10) -> 1
	// high (11~16) -> 0
	var p int
	if priority >= 11 {
		p = 0
	} else if priority >= 6 {
		p = 1
	} else {
		p = 2
	}
	return p
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
	trackLock   syncutil.Mutex
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
		tracker.SetGroupConfig(float64(group.GetRUSettings().GetRU().GetSettings().FillRate), group.Priority)

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
	km.RLock()
	defer km.RUnlock()

	group, ok := km.groups[c.resourceGroupName]
	if !ok {
		return
	}

	if c.isBackground || c.isTiFlash {
		return
	}

	km.trackLock.Lock()
	tracker, ok := km.groupTokens[c.resourceGroupName]
	if !ok {
		fillRate := group.getFillRate()
		tracker = &ResourceGroupTokenTracker{
			fillRate:         fillRate,
			priority:         groupPriority2TrackerPriority(group.Priority),
			overrideRUPerSec: fillRate,
			consumeTokenWindow: SlideWindow{
				sampleSeconds: 5,
			},
		}
		km.groupTokens[c.resourceGroupName] = tracker
	}
	tracker.consumeTokenWindow.Observe(c.RRU + c.WRU + c.ExpectedWaitRu)
	km.trackLock.Unlock()
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
	start := time.Now()

	km.RLock()
	defer km.RUnlock()

	totalLatestTokens := 0.
	totalAvgTokens := 0.
	totalActiveRU := 0.
	var priorityActiveTokens [3]float64

	type trackedGroup struct {
		tracker           *ResourceGroupTokenTracker
		group             *ResourceGroup
		requiredTokenRate float64
		normedTokens      float64 // requiredTokenRate/fillRate
	}

	var priorityTrackerGroup [3][]trackedGroup

	now := time.Now()
	changed := false
	changes := make([]*GroupRuRate, 0)
	km.trackLock.Lock()
	for name, track := range km.groupTokens {
		if now.Unix()-track.consumeTokenWindow.lastSampleTime > track.consumeTokenWindow.sampleSeconds {
			// newFillRate := min(track.fillRate, km.RULimit)
			// if track.overrideRUPerSec != newFillRate {
			// 	track.overrideRUPerSec = newFillRate
			// 	km.groups[name].SetOverrideFillRate(newFillRate)
			// 	changed = true
			// 	changes = append(changes, &GroupRuRate{
			// 		groupName: name,
			// 		fillRate:  newFillRate,
			// 		realRate:  newFillRate,
			// 	})
			// }
			continue
		}
		// ignore default currently.
		if name == "default" {
			continue
		}

		requiredTokenRate := min(track.consumeTokenWindow.AvgSamplePerSec(), track.fillRate)

		priorityTrackerGroup[track.priority] = append(priorityTrackerGroup[track.priority], trackedGroup{
			tracker:           track,
			group:             km.groups[name],
			normedTokens:      requiredTokenRate / track.fillRate,
			requiredTokenRate: requiredTokenRate,
		})

		priorityActiveTokens[track.priority] += requiredTokenRate
		totalLatestTokens += requiredTokenRate
		totalAvgTokens += track.consumeTokenWindow.AvgSamplePerSec()
		totalActiveRU += track.fillRate
	}
	for _, groups := range priorityTrackerGroup {
		sort.Slice(groups, func(i, j int) bool {
			return groups[i].normedTokens <= groups[j].normedTokens
		})
	}

	priorityThreshold := [3]float64{0.7 * km.RULimit, 0.2 * km.RULimit, 0.1 * km.RULimit}

	var priorityExpectedActiveTokens [3]float64
	for i := range len(priorityExpectedActiveTokens) {
		priorityExpectedActiveTokens[i] = min(priorityActiveTokens[i], priorityThreshold[i])
	}

	var priorityRealLimit [3]float64
	curTotalLimit := km.RULimit
	for i := range 3 {
		restReserved := 0.0
		for j := i + 1; j < 3; j++ {
			restReserved += priorityExpectedActiveTokens[j]
		}
		priorityRealLimit[i] = max(curTotalLimit-restReserved, priorityThreshold[i])
		curTotalLimit -= min(priorityRealLimit[i], priorityActiveTokens[i])
	}

	type groupFillRate struct {
		group    *ResourceGroup
		fillRate float64
	}

	updateGroups := make([]groupFillRate, 0)
	for priority, groups := range priorityTrackerGroup {
		priorityCurLimit := priorityRealLimit[priority]
		totalFillRate := 0.
		for _, g := range groups {
			totalFillRate += g.tracker.fillRate
		}

		for _, g := range groups {
			expectedTokens := min(priorityCurLimit*g.tracker.fillRate/totalFillRate, g.tracker.fillRate)
			if expectedTokens < g.tracker.overrideRUPerSec*0.95 || expectedTokens > g.tracker.overrideRUPerSec*1.05 {
				g.tracker.overrideRUPerSec = expectedTokens
				updateGroups = append(updateGroups, groupFillRate{
					group:    g.group,
					fillRate: expectedTokens,
				})
				changed = true
			}
			changes = append(changes, &GroupRuRate{
				groupName: g.group.Name,
				fillRate:  g.tracker.fillRate,
				realRate:  g.tracker.overrideRUPerSec,
			})

			totalFillRate -= g.tracker.fillRate
			priorityCurLimit -= min(expectedTokens, g.requiredTokenRate)
		}
	}
	km.trackLock.Unlock()

	for _, g := range updateGroups {
		g.group.SetOverrideFillRate(g.fillRate)
	}

	dur := time.Since(start)

	if changed || dur >= time.Second {
		log.Info("adjust keyspace fillrate", zap.Uint32("keyspace", km.ID), zap.Array("groups", GroupRuRateArray(changes)),
			zap.Array("level_limit", float64Arr(priorityRealLimit[:])), zap.Duration("dur", dur))
	}
}

type GroupRuRate struct {
	groupName string
	fillRate  float64
	realRate  float64
}

func (g *GroupRuRate) String() string {
	return fmt.Sprintf("[%s] %f/%f", g.groupName, g.realRate, g.fillRate)
}

type GroupRuRateArray []*GroupRuRate

func (g GroupRuRateArray) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, r := range g {
		e.AppendString(r.String())
	}
	return nil
}

type float64Arr []float64

func (a float64Arr) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, r := range a {
		e.AppendFloat64(r)
	}
	return nil
}

const SAMPLE_COUNT = 5

type SlideWindow struct {
	samples            [SAMPLE_COUNT]float64
	lastIndex          int
	currentSample      float64
	currentSampleStart int64
	sampleSeconds      int64
	lastSampleTime     int64
}

func (w *SlideWindow) Observe(v float64) {
	now := time.Now().Unix()
	// try to fill missing samples with latest sample.
	dur := now - w.currentSampleStart
	if dur > w.sampleSeconds {
		sampleCount := min(dur/w.sampleSeconds, int64(len(w.samples)))
		for range sampleCount {
			w.samples[w.lastIndex] = w.currentSample
		}
		w.lastIndex = (w.lastIndex + 1) % 5
		w.currentSample = 0.
		w.currentSampleStart = now - now%w.sampleSeconds
	}
	w.currentSample += v
	w.lastSampleTime = now
}

func (w *SlideWindow) LatestSamplePerSec() float64 {
	return w.samples[w.lastIndex] / float64(w.sampleSeconds)
}

func (w *SlideWindow) AvgSamplePerSec() float64 {
	sum := 0.
	for _, s := range w.samples {
		sum += s
	}

	return sum / float64(int(w.sampleSeconds)*len(w.samples))
}

func (w *SlideWindow) LatestSampleTime() time.Time {
	return time.Unix(w.currentSampleStart, 0)
}

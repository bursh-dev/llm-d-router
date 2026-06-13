/*
Copyright 2026 The llm-d Authors.

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

package preemption

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrmetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/metrics"
)

const (
	// PreemptionScorerType is the type of the PreemptionScorer.
	PreemptionScorerType = "preemption-scorer"

	defaultAttributeKey     = "preemptions-total"
	defaultKVGate           = 0.7
	defaultFloorMin         = 0.5
	defaultDeepFloor        = 0.1
	defaultEffectDurationMs = 300
	defaultMaxEscalations   = 2
)

// Parameters defines the configurable knobs for the PreemptionScorer.
type Parameters struct {
	// AttributeKey is the endpoint attribute holding the cumulative preemption counter.
	AttributeKey string `json:"attributeKey"`
	// KVGate is the KV-cache-usage fraction a fresh preemption must coincide with to arm a penalty.
	KVGate float64 `json:"kvGate"`
	// FloorMin is the score at the instant of a single, isolated preemption.
	FloorMin float64 `json:"floorMin"`
	// DeepFloor is the score once a pod escalates to MaxEscalations consecutive preemptions.
	DeepFloor float64 `json:"deepFloor"`
	// EffectDurationMs is both the linear-decay length and the escalation persistence window, in milliseconds.
	EffectDurationMs int `json:"effectDurationMs"`
	// MaxEscalations is the count of consecutive in-window preemptions at which the floor reaches DeepFloor.
	MaxEscalations int `json:"maxEscalations"`
}

// podState is the per-endpoint state used to detect fresh preemptions and decay the penalty.
type podState struct {
	// lastCounter is the last cumulative counter observed, used to detect a fresh increment.
	lastCounter float64
	// lastPreemptionTime is the scrape UpdateTime of the most recent gated fresh delta; it drives the decay.
	lastPreemptionTime time.Time
	// escCount is the number of consecutive gated preemptions without the decay window lapsing.
	escCount int
	// activeFloor is the floor this episode decays from, derived from escCount at stamp time.
	activeFloor float64
}

// compile-time type assertion
var _ fwksched.Scorer = &PreemptionScorer{}

// PreemptionScorer steers the decode profile away from pods that are actively thrashing
// their KV cache, using vLLM's cumulative preemption counter as a bounded, self-clearing penalty.
type PreemptionScorer struct {
	typedName fwkplugin.TypedName

	attributeKey   string
	kvGate         float64
	floorMin       float64
	deepFloor      float64
	effectDuration time.Duration
	maxEscalations int

	now     func() time.Time
	podList fwkplugin.PodListFunc

	mu    sync.Mutex
	state map[string]*podState
}

// PreemptionScorerFactory defines the factory function for the PreemptionScorer.
func PreemptionScorerFactory(name string, rawParameters *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	params := Parameters{
		AttributeKey:     defaultAttributeKey,
		KVGate:           defaultKVGate,
		FloorMin:         defaultFloorMin,
		DeepFloor:        defaultDeepFloor,
		EffectDurationMs: defaultEffectDurationMs,
		MaxEscalations:   defaultMaxEscalations,
	}
	if rawParameters != nil {
		if err := rawParameters.Decode(&params); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' scorer - %w", PreemptionScorerType, err)
		}
	}

	if err := params.validate(); err != nil {
		return nil, fmt.Errorf("invalid parameters for the '%s' scorer - %w", PreemptionScorerType, err)
	}

	var podList fwkplugin.PodListFunc
	if handle != nil {
		podList = handle.PodList
	}

	return &PreemptionScorer{
		typedName:      fwkplugin.TypedName{Type: PreemptionScorerType, Name: name},
		attributeKey:   params.AttributeKey,
		kvGate:         params.KVGate,
		floorMin:       params.FloorMin,
		deepFloor:      params.DeepFloor,
		effectDuration: time.Duration(params.EffectDurationMs) * time.Millisecond,
		maxEscalations: params.MaxEscalations,
		now:            time.Now,
		podList:        podList,
		state:          make(map[string]*podState),
	}, nil
}

func (p *Parameters) validate() error {
	if p.AttributeKey == "" {
		return errors.New("attributeKey must not be empty")
	}
	if p.KVGate < 0 || p.KVGate > 1 {
		return fmt.Errorf("kvGate must be in [0,1], got %v", p.KVGate)
	}
	if !(p.DeepFloor >= 0 && p.DeepFloor <= p.FloorMin && p.FloorMin <= 1) {
		return fmt.Errorf("floors must satisfy 0 <= deepFloor <= floorMin <= 1, got deepFloor=%v floorMin=%v", p.DeepFloor, p.FloorMin)
	}
	if p.EffectDurationMs <= 0 {
		return fmt.Errorf("effectDurationMs must be > 0, got %d", p.EffectDurationMs)
	}
	if p.MaxEscalations < 1 {
		return fmt.Errorf("maxEscalations must be >= 1, got %d", p.MaxEscalations)
	}
	return nil
}

// TypedName returns the type and name tuple of this plugin instance.
func (s *PreemptionScorer) TypedName() fwkplugin.TypedName {
	return s.typedName
}

// WithName sets the name of the scorer.
func (s *PreemptionScorer) WithName(name string) *PreemptionScorer {
	s.typedName.Name = name
	return s
}

// Category returns the preference the scorer applies when scoring candidate endpoints.
func (s *PreemptionScorer) Category() fwksched.ScorerCategory {
	return fwksched.Distribution
}

// Score returns the scoring result for the given endpoints. A pod that has never preempted,
// has no usable metrics snapshot, or whose penalty has fully decayed scores 1.0 (neutral);
// a pod that recently preempted under memory pressure scores down toward its active floor.
func (s *PreemptionScorer) Score(ctx context.Context, _ *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) map[fwksched.Endpoint]float64 {
	logger := log.FromContext(ctx).V(logging.DEBUG)
	now := s.now()

	scores := make(map[fwksched.Endpoint]float64, len(endpoints))
	seen := make(map[string]struct{}, len(endpoints))

	s.mu.Lock()
	for _, endpoint := range endpoints {
		key := endpoint.GetMetadata().NamespacedName.String()
		seen[key] = struct{}{}
		scores[endpoint] = s.scoreEndpoint(endpoint, key, now, logger)
	}
	s.prune(seen)
	s.mu.Unlock()

	return scores
}

// scoreEndpoint applies the fresh-delta observation and returns the decayed score for one endpoint.
// It must be called with s.mu held.
func (s *PreemptionScorer) scoreEndpoint(endpoint fwksched.Endpoint, key string, now time.Time, logger logr.Logger) float64 {
	cur, ok := attrmetrics.ReadScalarMetricValue(endpoint, s.attributeKey)
	metrics := endpoint.GetMetrics()
	if !ok || metrics == nil || metrics.UpdateTime.IsZero() {
		return 1.0
	}

	st, exists := s.state[key]
	if !exists {
		// First observation only establishes a baseline; a nonzero cumulative count
		// reflects past preemptions and must not arm a penalty.
		s.state[key] = &podState{lastCounter: float64(cur)}
		return 1.0
	}

	delta := float64(cur) - st.lastCounter
	if delta < 0 {
		// Counter reset (pod restart): clamp so a restart never reads as a fresh preemption.
		delta = 0
	}
	st.lastCounter = float64(cur)

	if delta > 0 && metrics.KVCacheUsagePercent > s.kvGate {
		if !st.lastPreemptionTime.IsZero() && metrics.UpdateTime.Sub(st.lastPreemptionTime) < s.effectDuration {
			st.escCount = min(st.escCount+1, s.maxEscalations)
		} else {
			st.escCount = 1
		}
		st.activeFloor = s.deriveFloor(st.escCount)
		st.lastPreemptionTime = metrics.UpdateTime
		logger.Info("preemption penalty armed", "endpoint", key, "escCount", st.escCount, "activeFloor", st.activeFloor)
	}

	return s.decayScore(st, now)
}

// deriveFloor ramps the floor linearly from floorMin (escCount=1) to deepFloor (escCount=maxEscalations).
func (s *PreemptionScorer) deriveFloor(escCount int) float64 {
	if s.maxEscalations == 1 {
		return s.floorMin
	}
	return s.floorMin - (s.floorMin-s.deepFloor)*float64(escCount-1)/float64(s.maxEscalations-1)
}

// decayScore maps the stored stamp's age to [activeFloor, 1.0] by linear decay over effectDuration.
func (s *PreemptionScorer) decayScore(st *podState, now time.Time) float64 {
	if st.lastPreemptionTime.IsZero() {
		return 1.0
	}
	penaltyFrac := 1.0 - float64(now.Sub(st.lastPreemptionTime))/float64(s.effectDuration)
	if penaltyFrac < 0 {
		penaltyFrac = 0
	} else if penaltyFrac > 1 {
		penaltyFrac = 1
	}
	return 1.0 - penaltyFrac*(1.0-st.activeFloor)
}

// prune drops bounded state for endpoints no longer relevant. With a pod source it removes
// entries absent from the datastore; without one it removes entries absent from the current
// candidates whose penalty has already decayed.
func (s *PreemptionScorer) prune(seen map[string]struct{}) {
	if live := s.livePods(); live != nil {
		for key := range s.state {
			if _, ok := live[key]; ok {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			delete(s.state, key)
		}
		return
	}

	now := s.now()
	for key, st := range s.state {
		if _, ok := seen[key]; ok {
			continue
		}
		if st.lastPreemptionTime.IsZero() || now.Sub(st.lastPreemptionTime) >= s.effectDuration {
			delete(s.state, key)
		}
	}
}

// livePods returns the set of pod keys reported by the handle, or nil when no pod source is configured.
func (s *PreemptionScorer) livePods() map[string]struct{} {
	if s.podList == nil {
		return nil
	}
	pods := s.podList()
	if pods == nil {
		return nil
	}
	live := make(map[string]struct{}, len(pods))
	for _, pod := range pods {
		live[pod.String()] = struct{}{}
	}
	return live
}

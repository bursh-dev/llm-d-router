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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrmetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/metrics"
)

// base is an arbitrary fixed wall-clock origin; t=0 in the worked examples.
var base = time.Date(2026, 6, 13, 0, 0, 0, 0, time.UTC)

// at returns base + ms milliseconds.
func at(ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }

// newEndpoint builds a candidate endpoint with an optional preemption counter, KV usage, and update time.
// A nil counter omits the attribute entirely (simulating a missing custom metric).
func newEndpoint(name string, counter *float64, kvUsage float64, updateTime time.Time) fwksched.Endpoint {
	attr := fwkdl.NewAttributes()
	if counter != nil {
		attr.Put(attrmetrics.ScalarMetricDataKey(defaultAttributeKey), attrmetrics.ScalarMetricValue(*counter))
	}
	meta := &fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: name, Namespace: "default"}}
	metrics := &fwkdl.Metrics{KVCacheUsagePercent: kvUsage, UpdateTime: updateTime}
	return fwksched.NewEndpoint(meta, metrics, attr)
}

func ptr(v float64) *float64 { return &v }

// newTestScorer builds a scorer at the defaults with a controllable clock and no pod source.
func newTestScorer() *PreemptionScorer {
	return &PreemptionScorer{
		typedName:      fwkplugin.TypedName{Type: PreemptionScorerType, Name: PreemptionScorerType},
		dataKey:        attrmetrics.ScalarMetricDataKey(defaultAttributeKey),
		kvGate:         defaultKVGate,
		floorMin:       defaultFloorMin,
		deepFloor:      defaultDeepFloor,
		effectDuration: defaultEffectDurationMs * time.Millisecond,
		maxEscalations: defaultMaxEscalations,
		now:            func() time.Time { return base },
		state:          make(map[string]*podState),
	}
}

// score runs a single-endpoint Score call at wall-clock now and returns the score.
func score(s *PreemptionScorer, ep fwksched.Endpoint, now time.Time) float64 {
	s.now = func() time.Time { return now }
	scores := s.Score(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{ep})
	return scores[ep]
}

func TestScore_NeutralCases(t *testing.T) {
	highKV := 0.9
	tests := []struct {
		name string
		ep   fwksched.Endpoint
	}{
		{"no preemption counter attribute", newEndpoint("pod", nil, highKV, base)},
		{"zero update time", newEndpoint("pod", ptr(5), highKV, time.Time{})},
		{"never preempted, zero counter", newEndpoint("pod", ptr(0), highKV, base)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestScorer()
			assert.Equal(t, 1.0, score(s, tc.ep, base))
		})
	}
}

func TestScore_NilMetricsNeutral(t *testing.T) {
	s := newTestScorer()
	attr := fwkdl.NewAttributes()
	attr.Put(attrmetrics.ScalarMetricDataKey(defaultAttributeKey), attrmetrics.ScalarMetricValue(3))
	meta := &fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod", Namespace: "default"}}
	ep := fwksched.NewEndpoint(meta, nil, attr)
	assert.Equal(t, 1.0, score(s, ep, base))
}

func TestScore_FirstObservationEstablishesBaseline(t *testing.T) {
	s := newTestScorer()
	// A pod first seen with a nonzero cumulative count must not be penalized for past preemptions,
	// even at high KV.
	ep := newEndpoint("pod", ptr(7), 0.95, base)
	assert.Equal(t, 1.0, score(s, ep, base))

	// A subsequent scrape with no further increment stays neutral.
	ep2 := newEndpoint("pod", ptr(7), 0.95, at(50))
	assert.Equal(t, 1.0, score(s, ep2, at(50)))
}

func TestScore_SinglePreemptionHighKVDecays(t *testing.T) {
	s := newTestScorer()
	// Baseline.
	assert.Equal(t, 1.0, score(s, newEndpoint("pod", ptr(0), 0.9, base), base))

	// Fresh delta at t0 under high KV arms floorMin.
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod", ptr(1), 0.9, at(0)), at(0)), 0.0001)

	// Linear decay back to neutral over effectDurationMs. The counter stays at 1 (no new delta).
	assert.InDelta(t, 0.67, score(s, newEndpoint("pod", ptr(1), 0.9, at(0)), at(100)), 0.01)
	assert.InDelta(t, 0.83, score(s, newEndpoint("pod", ptr(1), 0.9, at(0)), at(200)), 0.01)
	assert.InDelta(t, 1.00, score(s, newEndpoint("pod", ptr(1), 0.9, at(0)), at(300)), 0.0001)
	assert.InDelta(t, 1.00, score(s, newEndpoint("pod", ptr(1), 0.9, at(0)), at(400)), 0.0001)
}

func TestScore_LowKVGatesOutFreshDelta(t *testing.T) {
	s := newTestScorer()
	assert.Equal(t, 1.0, score(s, newEndpoint("pod", ptr(0), 0.5, base), base))
	// Fresh delta but KV below the gate: no penalty armed.
	assert.Equal(t, 1.0, score(s, newEndpoint("pod", ptr(1), 0.5, at(0)), at(0)))
}

func TestScore_LowKVDoesNotRestampExistingPenalty(t *testing.T) {
	s := newTestScorer()
	assert.Equal(t, 1.0, score(s, newEndpoint("pod", ptr(0), 0.9, base), base))
	// Arm at t0.
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod", ptr(1), 0.9, at(0)), at(0)), 0.0001)
	// A fresh delta at low KV at t100 must NOT re-stamp; the existing penalty keeps decaying.
	got := score(s, newEndpoint("pod", ptr(2), 0.5, at(100)), at(100))
	assert.InDelta(t, 0.67, got, 0.01, "low-KV fresh delta should not re-stamp")
}

func TestScore_SecondPreemptionInWindowEscalates(t *testing.T) {
	s := newTestScorer()
	assert.Equal(t, 1.0, score(s, newEndpoint("pod", ptr(0), 0.9, base), base))
	// #1 after calm -> floorMin at t0.
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod", ptr(1), 0.9, at(0)), at(0)), 0.0001)
	// #2 inside the window at t100 -> deepFloor, clock restarts at t100.
	assert.InDelta(t, 0.10, score(s, newEndpoint("pod", ptr(2), 0.9, at(100)), at(100)), 0.0001)
	// Decay from the deep floor over one window from t100.
	assert.InDelta(t, 0.55, score(s, newEndpoint("pod", ptr(2), 0.9, at(100)), at(250)), 0.0001)
	assert.InDelta(t, 1.00, score(s, newEndpoint("pod", ptr(2), 0.9, at(100)), at(400)), 0.0001)
}

func TestScore_SecondPreemptionAfterWindowReArmsFloorMin(t *testing.T) {
	s := newTestScorer()
	assert.Equal(t, 1.0, score(s, newEndpoint("pod", ptr(0), 0.9, base), base))
	// #1 at t0.
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod", ptr(1), 0.9, at(0)), at(0)), 0.0001)
	// Penalty fully decayed by t300.
	assert.InDelta(t, 1.00, score(s, newEndpoint("pod", ptr(1), 0.9, at(0)), at(300)), 0.0001)
	// #2 after the window at t400 -> floorMin again, not deepFloor.
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod", ptr(2), 0.9, at(400)), at(400)), 0.0001)
}

func TestScore_SustainedThrashThenRecover(t *testing.T) {
	s := newTestScorer()
	assert.Equal(t, 1.0, score(s, newEndpoint("pod", ptr(0), 0.9, base), base))
	// One preemption every 50ms scrape.
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod", ptr(1), 0.9, at(0)), at(0)), 0.0001)
	assert.InDelta(t, 0.10, score(s, newEndpoint("pod", ptr(2), 0.9, at(50)), at(50)), 0.0001)
	assert.InDelta(t, 0.10, score(s, newEndpoint("pod", ptr(3), 0.9, at(100)), at(100)), 0.0001)
	assert.InDelta(t, 0.10, score(s, newEndpoint("pod", ptr(4), 0.9, at(150)), at(150)), 0.0001)
	// Thrash stops at t150; decay from deepFloor over one window.
	assert.InDelta(t, 0.55, score(s, newEndpoint("pod", ptr(4), 0.9, at(150)), at(300)), 0.0001)
	assert.InDelta(t, 1.00, score(s, newEndpoint("pod", ptr(4), 0.9, at(150)), at(450)), 0.0001)
}

func TestScore_DeltaTwoBehavesLikeDeltaOne(t *testing.T) {
	s := newTestScorer()
	assert.Equal(t, 1.0, score(s, newEndpoint("pod", ptr(0), 0.9, base), base))
	// delta=2 in one scrape arms the same floorMin as delta=1 (no within-scrape escalation).
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod", ptr(2), 0.9, at(0)), at(0)), 0.0001)
}

func TestScore_CounterResetNoPenalty(t *testing.T) {
	s := newTestScorer()
	assert.Equal(t, 1.0, score(s, newEndpoint("pod", ptr(7), 0.9, base), base))
	// Counter drops 7 -> 0 (restart): delta clamped, no stamp, neutral.
	assert.Equal(t, 1.0, score(s, newEndpoint("pod", ptr(0), 0.9, at(50)), at(50)))
	// The next genuine increment from the new baseline is detected normally.
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod", ptr(1), 0.9, at(100)), at(100)), 0.0001)
}

func TestScore_DuplicateSameUpdateTimeIsNoOp(t *testing.T) {
	s := newTestScorer()
	assert.Equal(t, 1.0, score(s, newEndpoint("pod", ptr(0), 0.9, base), base))
	// Arm at t0.
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod", ptr(1), 0.9, at(0)), at(0)), 0.0001)
	st := s.state["default/pod"]
	require.NotNil(t, st)
	require.Equal(t, 1, st.escCount)
	// Re-scoring the identical scrape (same counter, same UpdateTime) must not double-increment.
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod", ptr(1), 0.9, at(0)), at(0)), 0.0001)
	assert.Equal(t, 1, st.escCount, "escCount must not double-increment on a repeat scrape")
}

func TestScore_MaxEscalationsThreeGradedFloors(t *testing.T) {
	s := newTestScorer()
	s.maxEscalations = 3
	assert.Equal(t, 1.0, score(s, newEndpoint("pod", ptr(0), 0.9, base), base))
	// escCount 1/2/3 -> floorMin(0.5)/midpoint(0.3)/deepFloor(0.1), capped at 3.
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod", ptr(1), 0.9, at(0)), at(0)), 0.0001)
	assert.InDelta(t, 0.30, score(s, newEndpoint("pod", ptr(2), 0.9, at(50)), at(50)), 0.0001)
	assert.InDelta(t, 0.10, score(s, newEndpoint("pod", ptr(3), 0.9, at(100)), at(100)), 0.0001)
	assert.InDelta(t, 0.10, score(s, newEndpoint("pod", ptr(4), 0.9, at(150)), at(150)), 0.0001)
}

func TestScore_MaxEscalationsOneDisablesEscalation(t *testing.T) {
	s := newTestScorer()
	s.maxEscalations = 1
	assert.Equal(t, 1.0, score(s, newEndpoint("pod", ptr(0), 0.9, base), base))
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod", ptr(1), 0.9, at(0)), at(0)), 0.0001)
	// A second in-window preemption stays at floorMin (no escalation to deepFloor).
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod", ptr(2), 0.9, at(50)), at(50)), 0.0001)
}

func TestScore_MultipleEndpointsIndependent(t *testing.T) {
	s := newTestScorer()
	a := newEndpoint("pod-a", ptr(0), 0.9, base)
	b := newEndpoint("pod-b", ptr(0), 0.9, base)
	s.now = func() time.Time { return base }
	first := s.Score(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{a, b})
	assert.Equal(t, 1.0, first[a])
	assert.Equal(t, 1.0, first[b])

	// pod-a preempts, pod-b stays calm.
	a2 := newEndpoint("pod-a", ptr(1), 0.9, at(0))
	b2 := newEndpoint("pod-b", ptr(0), 0.9, at(0))
	s.now = func() time.Time { return at(0) }
	second := s.Score(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{a2, b2})
	assert.InDelta(t, 0.50, second[a2], 0.0001)
	assert.Equal(t, 1.0, second[b2])
}

func TestPrune_FallbackRemovesDecayedAbsentEndpoints(t *testing.T) {
	s := newTestScorer()
	// Arm a penalty on pod-old at t0.
	assert.Equal(t, 1.0, score(s, newEndpoint("pod-old", ptr(0), 0.9, base), base))
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod-old", ptr(1), 0.9, at(0)), at(0)), 0.0001)
	require.NotNil(t, s.state["default/pod-old"])

	// Score only pod-new well past the decay window. pod-old is absent and fully decayed -> pruned.
	score(s, newEndpoint("pod-new", ptr(0), 0.9, at(1000)), at(1000))
	_, exists := s.state["default/pod-old"]
	assert.False(t, exists, "decayed absent endpoint should be pruned without a pod source")
}

func TestPrune_FallbackKeepsActiveAbsentEndpoints(t *testing.T) {
	s := newTestScorer()
	assert.Equal(t, 1.0, score(s, newEndpoint("pod-old", ptr(0), 0.9, base), base))
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod-old", ptr(1), 0.9, at(0)), at(0)), 0.0001)

	// Score pod-new while pod-old's penalty is still in effect -> pod-old retained.
	score(s, newEndpoint("pod-new", ptr(0), 0.9, at(100)), at(100))
	_, exists := s.state["default/pod-old"]
	assert.True(t, exists, "absent endpoint with an active penalty should be retained")
}

func TestPrune_PodListRemovesAbsentEndpoints(t *testing.T) {
	s := newTestScorer()
	live := []k8stypes.NamespacedName{{Name: "pod-live", Namespace: "default"}}
	s.podList = func() []k8stypes.NamespacedName { return live }

	// pod-gone arms a penalty but is not in the live pod list.
	assert.Equal(t, 1.0, score(s, newEndpoint("pod-gone", ptr(0), 0.9, base), base))
	assert.InDelta(t, 0.50, score(s, newEndpoint("pod-gone", ptr(1), 0.9, at(0)), at(0)), 0.0001)

	// Any subsequent Score call prunes entries absent from PodList, regardless of decay.
	score(s, newEndpoint("pod-live", ptr(0), 0.9, at(10)), at(10))
	_, exists := s.state["default/pod-gone"]
	assert.False(t, exists, "endpoint absent from PodList should be pruned even if penalty is active")
	_, live2 := s.state["default/pod-live"]
	assert.True(t, live2)
}

func TestFactory_Defaults(t *testing.T) {
	p, err := PreemptionScorerFactory("preempt", nil, nil)
	require.NoError(t, err)
	s, ok := p.(*PreemptionScorer)
	require.True(t, ok)
	assert.Equal(t, PreemptionScorerType, s.typedName.Type)
	assert.Equal(t, "preempt", s.typedName.Name)
	assert.Equal(t, attrmetrics.ScalarMetricDataKey(defaultAttributeKey), s.dataKey)
	assert.Equal(t, defaultKVGate, s.kvGate)
	assert.Equal(t, defaultFloorMin, s.floorMin)
	assert.Equal(t, defaultDeepFloor, s.deepFloor)
	assert.Equal(t, defaultEffectDurationMs*time.Millisecond, s.effectDuration)
	assert.Equal(t, defaultMaxEscalations, s.maxEscalations)
	assert.Equal(t, fwksched.Distribution, s.Category())
}

func TestFactory_ParsesParameters(t *testing.T) {
	cfg := `{"attributeKey":"preempt-x","kvGate":0.8,"floorMin":0.6,"deepFloor":0.2,"effectDurationMs":500,"maxEscalations":3}`
	dec := json.NewDecoder(strings.NewReader(cfg))
	p, err := PreemptionScorerFactory("preempt", dec, nil)
	require.NoError(t, err)
	s := p.(*PreemptionScorer)
	assert.Equal(t, attrmetrics.ScalarMetricDataKey("preempt-x"), s.dataKey)
	assert.Equal(t, 0.8, s.kvGate)
	assert.Equal(t, 0.6, s.floorMin)
	assert.Equal(t, 0.2, s.deepFloor)
	assert.Equal(t, 500*time.Millisecond, s.effectDuration)
	assert.Equal(t, 3, s.maxEscalations)
}

func TestFactory_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  string
	}{
		{"empty attributeKey", `{"attributeKey":""}`},
		{"kvGate too high", `{"kvGate":1.5}`},
		{"kvGate negative", `{"kvGate":-0.1}`},
		{"deepFloor above floorMin", `{"floorMin":0.3,"deepFloor":0.5}`},
		{"floorMin above 1", `{"floorMin":1.5,"deepFloor":0.1}`},
		{"deepFloor negative", `{"deepFloor":-0.1}`},
		{"effectDurationMs zero", `{"effectDurationMs":0}`},
		{"maxEscalations zero", `{"maxEscalations":0}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dec := json.NewDecoder(strings.NewReader(tc.cfg))
			_, err := PreemptionScorerFactory("preempt", dec, nil)
			assert.Error(t, err)
		})
	}
}

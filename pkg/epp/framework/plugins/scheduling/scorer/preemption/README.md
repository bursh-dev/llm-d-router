# Preemption-Aware Scorer

**Type:** `preemption-scorer`

Steers the decode profile away from pods that are actively thrashing their KV cache, using vLLM's
cumulative `vllm:num_preemptions_total` counter as a bounded, self-clearing penalty. It complements,
never overrides, the prefix/KV/queue scorers: a pod that has never preempted, has no usable metrics
snapshot, or whose penalty has fully decayed scores `1.0` (neutral).

## What it does

The cumulative counter is scraped onto each endpoint as a custom attribute (default
`preemptions-total`). For each candidate pod the scorer keeps a small per-pod state record and, on
every `Score` call:

- Detects a *fresh increment* of the counter (negative deltas from a pod restart are clamped to zero).
- On the first observation of a pod, records the counter as a baseline and scores `1.0`, so past
  preemptions never arm a penalty.
- Re-stamps the penalty only when a fresh increment coincides with KV-cache usage above `kvGate`
  (the leading memory-pressure gauge). A preemption that has already resolved at low KV never
  penalizes.
- A preemption that lands while a prior penalty is still in effect escalates the depth one step;
  the floor ramps linearly from `floorMin` (an isolated hit after calm) to `deepFloor` (sustained
  thrash at `maxEscalations` consecutive in-window preemptions).
- Scores by decaying the most recent stamp linearly back to neutral over `effectDurationMs`.

The depth comes from *recency and the count of consecutive scrapes*, not from the magnitude of a
single delta: a `delta=2` scrape bites the same as `delta=1`. After thrashing stops, the pod decays
back to `1.0` over exactly one `effectDurationMs` window.

## Scheduling intent

The scorer returns category `Distribution`. It is a late corrective term, not a primary router
signal: the leading gauges (`num_requests_waiting`, `kv_cache_usage_perc`) already steer traffic
before preemptions occur. Recommended weight `0.5`, well below the prefix-cache weight (`2.0`) and
at or below the queue/kv scorers (`1.0`).

## Inputs consumed

- `KVCacheUsagePercent` from endpoint metrics (standard slot).
- The cumulative preemption counter from the endpoint attribute named by `attributeKey`. This must
  be configured on the metrics extractor via a `customMetrics` entry; absent it, every pod scores
  `1.0`.

## Parameters

- `attributeKey` (string, default `preemptions-total`): endpoint attribute holding the cumulative
  preemption counter.
- `kvGate` (float, default `0.7`): KV-cache-usage fraction a fresh preemption must coincide with to
  arm a penalty. Must be in `[0,1]`.
- `floorMin` (float, default `0.5`): score at the instant of a single, isolated preemption. `1.0`
  disables the single-hit penalty.
- `deepFloor` (float, default `0.1`): score once a pod escalates to `maxEscalations` consecutive
  preemptions. Must satisfy `0 <= deepFloor <= floorMin <= 1`; `deepFloor = floorMin` disables
  escalation.
- `effectDurationMs` (int, default `300`): both the linear-decay length and the persistence window
  for escalation (a second preemption within it counts as thrash). Must be `> 0`.
- `maxEscalations` (int, default `2`): consecutive in-window preemptions at which the floor reaches
  `deepFloor`. `2` is the two-level default; `N > 2` inserts evenly-spaced intermediate floors;
  `1` pins `floorMin` and disables escalation. Must be `>= 1`.

## Configuration Example

The extractor must publish the counter. Note `engineConfigs` replaces an engine's spec wholesale, so
the standard vLLM specs are repeated alongside `customMetrics`:

```yaml
plugins:
  - type: core-metrics-extractor
    parameters:
      engineConfigs:
        - name: vllm
          queuedRequestsSpec: vllm:num_requests_waiting
          runningRequestsSpec: vllm:num_requests_running
          kvUsageSpec: vllm:kv_cache_usage_perc
          loraSpec: vllm:lora_requests_info
          cacheInfoSpec: vllm:cache_config_info
          customMetrics:
            - attributeKey: preemptions-total
              metricSpec: vllm:num_preemptions_total
  - type: preemption-scorer
    parameters:
      attributeKey: preemptions-total
      kvGate: 0.7
      floorMin: 0.5
      deepFloor: 0.1
      effectDurationMs: 300
      maxEscalations: 2
schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: preemption-scorer
        weight: 0.5
```

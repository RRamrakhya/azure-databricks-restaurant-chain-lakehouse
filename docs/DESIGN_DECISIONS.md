# Design decisions (ADR-style)

**ADR-1 Source abstraction (`source_mode`).** Context: Event Hub isn't available on Free Edition. Decision: ingest raw JSON strings first (`live_orders_raw`) from either Kafka or Auto Loader, parse downstream. Consequence: identical downstream logic; raw payload retained for replay.

**ADR-2 Quarantine instead of drop at Bronze.** Context: dropping malformed events loses evidence and hides producer bugs. Decision: contract-violating events go to `live_orders_quarantine` with reason codes. Alert on quarantine rate. Trade-off: extra table and rule duplication (contract JSON + SQL rules), covered by a CI test that checks field coverage.

**ADR-3 Idempotent upsert for facts.** Event Hubs gives at-least-once delivery, so duplicates are expected. `apply_changes` keyed on `order_id` (sequence by ingest/ order time) makes re-delivery harmless and merges live + historical in one target, replacing `UNION ALL` + manual dedupe.

**ADR-4 Expectation policy by blast radius.** Fact rows: DROP (isolated bad rows). Dimension keys: FAIL (a null key corrupts every join). AI-derived sentiment: WARN (non-critical, monitored). Each choice is documented because "why drop vs fail" is a standard interview probe.

**ADR-5 LLM output treated as untrusted.** Strip fences via regex, validate enums, store `ai_json_valid` and `prompt_version`, evaluate against a golden set and a keyword baseline before changing prompt/model.

**ADR-6 Triggered pipelines, not a continuous stream in a DAG.** A continuous task never "completes", so downstream tasks never run. Use triggered runs that drain available data, or separate continuous ingestion from the scheduled transform.

**ADR-7 Liquid clustering over date partitioning.** Daily partitions on small tables create small files; clustering adapts to query patterns.

**ADR-8 Compute choices.** Serverless for pipelines/SQL. Classic compute only where always-on gateways require it (Azure mode). Cluster policy caps gateway cost (design-only).

**Observed baseline weakness:** on the synthetic data the keyword baseline over-flags praise such as "great portion" as a portion complaint, which is the kind of error the evaluation harness is meant to surface.

# ANSWERS.md — Day 28 Track 2

## Trade-offs and Production Gaps

### 1. Architecture Decisions

#### IP01-IP02: HTTP → Kafka Ingestion
**Trade-off:** Using Kafka as buffer between HTTP API and Airflow pipeline
- **Pro:** Decouples producers from consumers; enables replay on failure
- **Con:** Adds latency and complexity; requires careful consumer group management
- **Production gap:** No schema registry enforcement on Kafka payloads

#### IP03: Delta MERGE for Idempotency
**Trade-off:** Delta MERGE with deduplication key vs. simple INSERT
- **Pro:** Handles Kafka replay without duplicate records; time travel for audit
- **Con:** Requires careful key selection; MERGE has different performance profile than INSERT
- **Production gap:** No automatic partition pruning optimization; large MERGE batches need tuning

#### IP04: Feast Feature Store
**Trade-off:** Online feature store vs. direct Delta queries
- **Pro:** Sub-millisecond feature retrieval; consistent feature definitions
- **Con:** Dual maintenance of offline (Delta) and online (Feast) pipelines
- **Production gap:** Feature freshness lag; need to monitor materialization delays

#### IP05: Qdrant Vector Store
**Trade-off:** Hybrid search vs. single vector type
- **Pro:** Combines dense (semantic) and sparse (keyword) retrieval
- **Con:** More complex index management; scoring normalization needed
- **Production gap:** Embedding model updates require re-indexing all documents

#### IP06: MLflow Model Registry
**Trade-off:** Alias-based promotion vs. version-based routing
- **Pro:** Atomic promotion/rollback; audit trail for model lifecycle
- **Con:** Requires disciplined alias management across environments
- **Production gap:** No automatic rollback on quality regression; needs human decision

#### IP07: vLLM Inference
**Trade-off:** Local GPU vs. managed inference endpoints
- **Pro:** Lower latency; no data leaves infrastructure
- **Con:** GPU cost and maintenance; scaling complexity
- **Production gap:** No automatic failover to backup endpoint; cold start time

#### IP08: Envoy Gateway
**Trade-off:** Edge gateway vs. direct service exposure
- **Pro:** Centralized auth, rate limiting, observability; single entry point
- **Con:** Additional hop; gateway becomes single point of failure
- **Production gap:** Rate limit configuration requires tuning; no automatic backpressure

#### IP09: Prometheus/Grafana Observability
**Trade-off:** Full metrics vs. sampled traces
- **Pro:** Complete visibility; actionable alerts
- **Con:** Storage cost for high-cardinality metrics
- **Production gap:** Alert fatigue risk; need SLO-based alerting

#### IP10: W3C Trace Context
**Trade-off:** End-to-end tracing vs. per-service logging
- **Pro:** Single view across all boundaries; debugging made easy
- **Con:** Propagates through Kafka (requires header injection); sampling strategy needed
- **Production gap:** Trace context loss when services don't propagate correctly

---

### 2. Production Readiness Gaps

| Area | Gap | Mitigation |
|------|-----|------------|
| **Security** | No mTLS between services | Configure service mesh certs |
| **Scaling** | No horizontal pod autoscaling | Add HPA manifests |
| **Backup** | No Delta table backup strategy | Configure S3 replication |
| **Secrets** | Plaintext in environment | Migrate to Vault/Sealed Secrets |
| **DR** | No cross-region failover | Multi-cluster setup |
| **Cost** | No cost attribution by team | Add labels to all resources |

---

### 3. Team Contributions

| Member | Responsibilities |
|--------|-----------------|
| **Individual** | All 4 integration functions (IP01, IP03, IP04, IP07/IP08) |
| - | `event_headers()`: traceparent + idempotency-key for Kafka |
| - | `dedupe_latest()`: replay-safe deduplication with newest-wins |
| - | `feast_online_request()`: Feast feature store request builder |
| - | `readiness_status()`: ready/degraded/not_ready semantics |

---

### 4. Questions for Reflection

1. **Why is idempotency key required at ingestion, not just at storage?**
   - Kafka provides at-least-once delivery; idempotency at ingestion prevents processing the same event twice regardless of where failure occurs.

2. **Why both Delta AND Feast?**
   - Delta: ACID transactions, time travel, batch processing
   - Feast: Sub-millisecond online feature serving; avoids query latency in serving path

3. **Why MLflow alias instead of version number?**
   - Alias allows atomic rollback; version numbers require code changes to route traffic

4. **What happens if Kafka loses messages between API and Airflow?**
   - Dead Letter Queue captures failures; manual replay available via `lab28 dlq --replay`

5. **How would you handle a bad model release?**
   - MLflow alias rollback: `mlflowClient.set_registered_model_alias()` without code change
   - Delta time travel to re-evaluate on historical data

---

*Generated: 2026-09-03*

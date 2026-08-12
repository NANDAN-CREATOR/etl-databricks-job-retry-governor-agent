# Day 26 — ETL Databricks Job Retry Governor Agent

## Series
**Agentic AI in Data Engineering** — Day 26 of 100

## Concept
An Ollama-powered LLM agent that governs Databricks job retries by enforcing a
**CLASSIFY-BEFORE-RETRY** guardrail. The agent investigates failed job runs,
collects cluster logs and Spark UI metrics, and uses tool-calling to classify the
failure before deciding whether a retry is appropriate.

## Guardrail: CLASSIFY-BEFORE-RETRY
The agent **must** call `classify_failure_type` before calling `trigger_retry`.
Failure types and retry policy:

| Failure Type | Example | Retryable | Strategy |
|---|---|---|---|
| `TRANSIENT` | Driver eviction, spot loss | ✅ Yes | Standard retry |
| `OOM` | GC overhead, heap exceeded | ✅ Yes | Upscaled cluster retry |
| `CONFIG` | Schema mismatch, missing column | ❌ No | Escalate to engineer |
| `BAD_DATA` | Bad records threshold exceeded | ❌ No | Escalate to data team |

## Agentic Pattern
- **LLM**: Ollama (`llama3.2`) with tool-calling loop
- **Tools**: 5 tools covering status → logs → metrics → classify → retry
- **Loop**: Continues until LLM emits a final text decision (no more tool calls)

## Tool Chain
```
get_job_run_status → get_cluster_logs → get_spark_ui_metrics
       → classify_failure_type (GUARDRAIL) → trigger_retry
```

## Test Scenarios
| Run ID | Error Type | Expected Outcome |
|---|---|---|
| run_101 | OOM / GC overhead | Retry with upscaled cluster |
| run_102 | CONFIG (schema error) | Blocked — escalate to engineer |
| run_103 | TRANSIENT (driver evicted) | Standard retry |
| run_104 | BAD_DATA (bad records) | Blocked — escalate to data team |

## Setup
```bash
pip install ollama
ollama pull llama3.2
python agent.py
```

## Stack
- Python 3.10+
- Ollama (`llama3.2`)
- Databricks Jobs API (mocked)
- Spark UI Metrics (mocked)

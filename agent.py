"""
Day 26 — ETL Databricks Job Retry Governor Agent
Guardrail: CLASSIFY-BEFORE-RETRY
Never retry a job without first classifying the failure type via LLM.
Config failures and bad-data failures are NEVER retried.
"""

import json
import ollama

# ── Mock tool implementations ─────────────────────────────────────────────────

def get_job_run_status(run_id: str) -> dict:
    """Fetch Databricks job run status and exit details."""
    mock = {
        "run_101": {"state": "FAILED", "result_state": "FAILED",
                    "exit_code": 1,
                    "error_message": "java.lang.OutOfMemoryError: GC overhead limit exceeded",
                    "duration_seconds": 430, "cluster_id": "cl-abc"},
        "run_102": {"state": "FAILED", "result_state": "FAILED",
                    "exit_code": 1,
                    "error_message": "AnalysisException: Column 'customer_id' does not exist in schema",
                    "duration_seconds": 12, "cluster_id": "cl-def"},
        "run_103": {"state": "FAILED", "result_state": "FAILED",
                    "exit_code": 1,
                    "error_message": "com.databricks.backend.daemon.driver.DriverClient$DriverNotFoundException: Driver unavailable",
                    "duration_seconds": 5, "cluster_id": "cl-ghi"},
        "run_104": {"state": "FAILED", "result_state": "FAILED",
                    "exit_code": 1,
                    "error_message": "BadRecordsException: 1200 bad records found; threshold exceeded (max 100)",
                    "duration_seconds": 220, "cluster_id": "cl-jkl"},
    }
    return mock.get(run_id, {"state": "UNKNOWN", "error_message": "Run not found"})


def get_cluster_logs(cluster_id: str) -> dict:
    """Fetch cluster-level logs for diagnosis."""
    mock = {
        "cl-abc": {"driver_heap_used_pct": 97, "executor_oom_count": 3,
                   "gc_time_pct": 42, "log_snippet": "GC overhead limit exceeded after 430s"},
        "cl-def": {"driver_heap_used_pct": 30, "executor_oom_count": 0,
                   "gc_time_pct": 2, "log_snippet": "AnalysisException at plan stage; job aborted in 12s"},
        "cl-ghi": {"driver_heap_used_pct": 0, "executor_oom_count": 0,
                   "gc_time_pct": 0, "log_snippet": "Driver process not found; likely spot eviction"},
        "cl-jkl": {"driver_heap_used_pct": 55, "executor_oom_count": 0,
                   "gc_time_pct": 5, "log_snippet": "1200 malformed JSON rows; bad-records path written"},
    }
    return mock.get(cluster_id, {"log_snippet": "No logs available"})


def get_spark_ui_metrics(run_id: str) -> dict:
    """Fetch key Spark UI metrics for the failed run."""
    mock = {
        "run_101": {"total_tasks": 800, "failed_tasks": 312, "spill_bytes": 18_000_000_000,
                    "shuffle_read_bytes": 4_200_000_000, "peak_executor_memory_gb": 14.8},
        "run_102": {"total_tasks": 0, "failed_tasks": 0, "spill_bytes": 0,
                    "shuffle_read_bytes": 0, "peak_executor_memory_gb": 0},
        "run_103": {"total_tasks": 0, "failed_tasks": 0, "spill_bytes": 0,
                    "shuffle_read_bytes": 0, "peak_executor_memory_gb": 0},
        "run_104": {"total_tasks": 1400, "failed_tasks": 0, "spill_bytes": 0,
                    "shuffle_read_bytes": 600_000_000, "peak_executor_memory_gb": 6.2},
    }
    return mock.get(run_id, {})


def classify_failure_type(run_id: str, error_message: str,
                           cluster_logs: dict, spark_metrics: dict) -> dict:
    """
    GUARDRAIL TOOL — LLM classifies failure before any retry decision.
    Returns: {failure_type, retryable, reason}
    failure_type: TRANSIENT | OOM | CONFIG | BAD_DATA
    """
    # This is the guardrail; actual classification done by LLM tool-calling loop
    return {
        "run_id": run_id,
        "error_message": error_message,
        "cluster_logs_summary": cluster_logs.get("log_snippet", ""),
        "oom_count": cluster_logs.get("executor_oom_count", 0),
        "spill_bytes": spark_metrics.get("spill_bytes", 0),
        "peak_memory_gb": spark_metrics.get("peak_executor_memory_gb", 0),
        "status": "classification_needed"
    }


def trigger_retry(run_id: str, failure_type: str, retry_strategy: str) -> dict:
    """Trigger a job retry with the appropriate strategy."""
    if failure_type in ("CONFIG", "BAD_DATA"):
        return {
            "status": "BLOCKED",
            "reason": f"Retry blocked by guardrail — failure_type={failure_type} requires human intervention",
            "run_id": run_id
        }
    strategies = {
        "TRANSIENT": "standard_retry",
        "OOM": "upscaled_cluster_retry"
    }
    action = strategies.get(failure_type, "standard_retry")
    return {
        "status": "RETRY_TRIGGERED",
        "run_id": run_id,
        "failure_type": failure_type,
        "strategy_applied": retry_strategy or action,
        "new_run_id": f"{run_id}_retry1"
    }


# ── Tool registry ─────────────────────────────────────────────────────────────

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_job_run_status",
            "description": "Fetch Databricks job run status and error message for a given run_id.",
            "parameters": {
                "type": "object",
                "properties": {"run_id": {"type": "string"}},
                "required": ["run_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_cluster_logs",
            "description": "Fetch cluster-level driver and executor logs for the cluster that ran the job.",
            "parameters": {
                "type": "object",
                "properties": {"cluster_id": {"type": "string"}},
                "required": ["cluster_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_spark_ui_metrics",
            "description": "Get Spark UI metrics (task counts, spill, shuffle, peak memory) for a run.",
            "parameters": {
                "type": "object",
                "properties": {"run_id": {"type": "string"}},
                "required": ["run_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "classify_failure_type",
            "description": (
                "GUARDRAIL: Classify the Databricks job failure type before any retry decision. "
                "Must be called before trigger_retry. "
                "Returns failure_type: TRANSIENT | OOM | CONFIG | BAD_DATA and whether it is retryable."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string"},
                    "error_message": {"type": "string"},
                    "cluster_logs": {"type": "object"},
                    "spark_metrics": {"type": "object"}
                },
                "required": ["run_id", "error_message", "cluster_logs", "spark_metrics"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "trigger_retry",
            "description": (
                "Trigger a Databricks job retry. Only call after classify_failure_type confirms retryable=true. "
                "CONFIG and BAD_DATA failures are blocked automatically."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string"},
                    "failure_type": {"type": "string", "enum": ["TRANSIENT", "OOM", "CONFIG", "BAD_DATA"]},
                    "retry_strategy": {"type": "string"}
                },
                "required": ["run_id", "failure_type", "retry_strategy"]
            }
        }
    }
]

TOOL_FN_MAP = {
    "get_job_run_status": get_job_run_status,
    "get_cluster_logs": get_cluster_logs,
    "get_spark_ui_metrics": get_spark_ui_metrics,
    "classify_failure_type": classify_failure_type,
    "trigger_retry": trigger_retry,
}

SYSTEM_PROMPT = """You are an ETL Databricks Job Retry Governor agent.

Your job: given a failed Databricks job run_id, investigate the failure and decide whether to retry.

GUARDRAIL — CLASSIFY-BEFORE-RETRY:
You MUST call classify_failure_type before trigger_retry. Never skip classification.
- TRANSIENT failures (driver eviction, network blip): safe to retry with standard strategy.
- OOM failures (heap exceeded, GC overhead): retry only with upscaled_cluster_retry strategy.
- CONFIG failures (schema mismatch, column not found): NEVER retry — escalate to engineer.
- BAD_DATA failures (bad records threshold exceeded): NEVER retry — escalate to data team.

Investigation sequence:
1. get_job_run_status → get cluster_id and error_message
2. get_cluster_logs(cluster_id) → heap, OOM count, GC
3. get_spark_ui_metrics(run_id) → spill, shuffle, peak memory
4. classify_failure_type → determine type and retryability
5. trigger_retry only if retryable; otherwise report escalation needed
"""


def run_agent(run_id: str):
    print(f"\n{'='*60}")
    print(f"  Retry Governor — Investigating: {run_id}")
    print(f"{'='*60}")

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"Investigate failed Databricks job run: {run_id}. Apply retry guardrail."}
    ]

    while True:
        response = ollama.chat(
            model="llama3.2",
            messages=messages,
            tools=TOOLS
        )
        msg = response["message"]
        messages.append(msg)

        if not msg.get("tool_calls"):
            print("\n[AGENT DECISION]")
            print(msg["content"])
            break

        for tc in msg["tool_calls"]:
            fn_name = tc["function"]["name"]
            fn_args = tc["function"]["arguments"]
            if isinstance(fn_args, str):
                fn_args = json.loads(fn_args)

            print(f"\n[TOOL] {fn_name}({json.dumps(fn_args, indent=2)})")
            result = TOOL_FN_MAP[fn_name](**fn_args)
            print(f"[RESULT] {json.dumps(result, indent=2)}")

            messages.append({
                "role": "tool",
                "content": json.dumps(result)
            })


if __name__ == "__main__":
    test_runs = ["run_101", "run_102", "run_103", "run_104"]
    for rid in test_runs:
        run_agent(rid)

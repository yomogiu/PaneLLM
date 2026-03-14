#!/usr/bin/env python3
from __future__ import annotations

import json
import select
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MLX_CHAT_CONTRACT_BASE = {
    "schema_version": "mlx_chat_v1",
    "message_format": "openai_chat_messages_v1",
    "tool_call_format": "none_v1",
    "chat_template_assumption": "qwen_jinja_default_or_plaintext_fallback_v1",
    "tokenizer_template_mode": "apply_chat_template_default_v1",
    "max_context_behavior": "tail_truncate_chars_v1",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit(payload: dict[str, Any]) -> int:
    sys.stdout.write(json.dumps(payload, ensure_ascii=True) + "\n")
    sys.stdout.flush()
    return 0


def fail(code: str, message: str, *, details: dict[str, Any] | None = None) -> int:
    return emit(
        {
            "ok": False,
            "error": {
                "code": code,
                "message": message,
                "details": details or {},
            },
            "created_at": now_iso(),
        }
    )


def summarize_mlx_worker_failure(detail: Any) -> str:
    text = " ".join(str(detail or "").split())[:600]
    if not text:
        return ""
    if "NSRangeException" in text and ("DeviceC2Ev" in text or "MetalAllocator" in text):
        return (
            "MLX crashed during Metal device initialization. "
            "The process does not appear to have a usable Metal device in this runtime."
        )
    return text


def stderr_excerpt(process: subprocess.Popen[str]) -> str:
    try:
        if not process.stderr:
            return ""
        text = process.stderr.read() or ""
    except Exception:
        return ""
    return summarize_mlx_worker_failure(text)


def build_contract(max_context_chars: int) -> dict[str, Any]:
    return {
        **MLX_CHAT_CONTRACT_BASE,
        "max_context_chars": int(max_context_chars),
    }


def read_worker_response(process: subprocess.Popen[str], expected_request_id: str, timeout_sec: float) -> dict[str, Any]:
    deadline = time.monotonic() + max(0.1, timeout_sec)
    fd = process.stdout.fileno()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            if process.poll() is not None:
                detail = stderr_excerpt(process)
                if detail:
                    raise RuntimeError(f"MLX worker exited before responding: {detail}")
            raise TimeoutError("Timed out waiting for MLX worker response.")
        ready, _, _ = select.select([fd], [], [], remaining)
        if not ready:
            if process.poll() is not None:
                detail = stderr_excerpt(process)
                if detail:
                    raise RuntimeError(f"MLX worker exited before responding: {detail}")
            continue
        line = process.stdout.readline()
        if line == "":
            detail = stderr_excerpt(process)
            if detail:
                raise RuntimeError(f"MLX worker closed its stdout stream: {detail}")
            raise RuntimeError("MLX worker closed its stdout stream.")
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        if str(payload.get("request_id", "")) == expected_request_id:
            return payload


def rpc(
    process: subprocess.Popen[str],
    *,
    op: str,
    payload: dict[str, Any],
    timeout_sec: float,
) -> dict[str, Any]:
    request_id = f"exp_{uuid.uuid4().hex[:12]}"
    process.stdin.write(json.dumps({"request_id": request_id, "op": op, **payload}, ensure_ascii=True) + "\n")
    process.stdin.flush()
    response = read_worker_response(process, request_id, timeout_sec)
    if not bool(response.get("ok")):
        error = response.get("error") if isinstance(response.get("error"), dict) else {}
        raise RuntimeError(str(error.get("message", "MLX worker request failed.")))
    data = response.get("data")
    return data if isinstance(data, dict) else {}


def normalize_prompt_set(raw_prompts: Any) -> list[dict[str, str]]:
    if not isinstance(raw_prompts, list) or not raw_prompts:
        raise ValueError("prompt_set must be a non-empty array.")
    normalized: list[dict[str, str]] = []
    for index, item in enumerate(raw_prompts[:16]):
        if isinstance(item, str):
            prompt = item.strip()
            reference = ""
            item_id = f"prompt_{index + 1:02d}"
        elif isinstance(item, dict):
            prompt = str(item.get("prompt", "")).strip()
            reference = str(item.get("reference", "")).strip()
            item_id = str(item.get("id", "")).strip() or f"prompt_{index + 1:02d}"
        else:
            continue
        if not prompt:
            continue
        normalized.append({"id": item_id, "prompt": prompt[:4000], "reference": reference[:1200]})
    if not normalized:
        raise ValueError("prompt_set must contain at least one non-empty prompt.")
    return normalized


def score_output(output: str, reference: str) -> dict[str, Any]:
    if not reference:
        return {"exact_match": None, "contains_reference": None}
    normalized_output = output.strip()
    normalized_reference = reference.strip()
    output_lower = normalized_output.lower()
    reference_lower = normalized_reference.lower()
    return {
        "exact_match": normalized_output == normalized_reference,
        "contains_reference": reference_lower in output_lower,
    }


def build_messages(system_prompt: str, prompt: str) -> list[dict[str, str]]:
    messages: list[dict[str, str]] = []
    if system_prompt.strip():
        messages.append({"role": "system", "content": system_prompt.strip()})
    messages.append({"role": "user", "content": prompt})
    return messages


def run_prompt_eval(
    process: subprocess.Popen[str],
    *,
    prompt_set: list[dict[str, str]],
    contract: dict[str, Any],
    generation: dict[str, Any],
    system_prompt: str,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    total_latency_ms = 0
    exact_match_hits = 0
    contains_reference_hits = 0
    exact_match_total = 0
    contains_reference_total = 0

    for item in prompt_set:
        result = rpc(
            process,
            op="generate",
            payload={
                "schema_version": contract["schema_version"],
                "contract": contract,
                "messages": build_messages(system_prompt, item["prompt"]),
                "params": generation,
            },
            timeout_sec=180,
        )
        output = str(result.get("text", "")).strip()
        latency_ms = int(result.get("latency_ms", 0) or 0)
        token_count = int(result.get("token_count", 0) or 0)
        metrics = score_output(output, item["reference"])
        if metrics["exact_match"] is not None:
            exact_match_total += 1
            exact_match_hits += 1 if metrics["exact_match"] else 0
        if metrics["contains_reference"] is not None:
            contains_reference_total += 1
            contains_reference_hits += 1 if metrics["contains_reference"] else 0
        total_latency_ms += latency_ms
        items.append(
            {
                "id": item["id"],
                "prompt": item["prompt"],
                "reference": item["reference"],
                "output": output,
                "token_count": token_count,
                "latency_ms": latency_ms,
                "metrics": metrics,
            }
        )

    prompt_count = len(items)
    return {
        "kind": "prompt_eval",
        "prompt_count": prompt_count,
        "items": items,
        "summary": {
            "prompt_count": prompt_count,
            "average_latency_ms": round(total_latency_ms / prompt_count, 2) if prompt_count else 0,
            "exact_match_rate": round(exact_match_hits / exact_match_total, 4) if exact_match_total else None,
            "contains_reference_rate": (
                round(contains_reference_hits / contains_reference_total, 4)
                if contains_reference_total
                else None
            ),
        },
    }


def run_adapter_eval(
    process: subprocess.Popen[str],
    *,
    prompt_set: list[dict[str, str]],
    contract: dict[str, Any],
    generation: dict[str, Any],
    system_prompt: str,
    adapter_path: str,
) -> dict[str, Any]:
    base_result = run_prompt_eval(
        process,
        prompt_set=prompt_set,
        contract=contract,
        generation=generation,
        system_prompt=system_prompt,
    )
    rpc(
        process,
        op="adapter_load",
        payload={"adapter_path": adapter_path},
        timeout_sec=120,
    )
    adapter_result = run_prompt_eval(
        process,
        prompt_set=prompt_set,
        contract=contract,
        generation=generation,
        system_prompt=system_prompt,
    )

    paired_items: list[dict[str, Any]] = []
    improved_contains_reference = 0
    comparable_items = 0
    for base_item, adapter_item in zip(base_result["items"], adapter_result["items"]):
        base_contains = base_item["metrics"].get("contains_reference")
        adapter_contains = adapter_item["metrics"].get("contains_reference")
        if base_contains is not None and adapter_contains is not None:
            comparable_items += 1
            if adapter_contains and not base_contains:
                improved_contains_reference += 1
        paired_items.append(
            {
                "id": base_item["id"],
                "prompt": base_item["prompt"],
                "reference": base_item["reference"],
                "base": {
                    "output": base_item["output"],
                    "token_count": base_item["token_count"],
                    "latency_ms": base_item["latency_ms"],
                    "metrics": base_item["metrics"],
                },
                "adapter": {
                    "output": adapter_item["output"],
                    "token_count": adapter_item["token_count"],
                    "latency_ms": adapter_item["latency_ms"],
                    "metrics": adapter_item["metrics"],
                },
                "changed_output": base_item["output"] != adapter_item["output"],
            }
        )

    return {
        "kind": "adapter_eval",
        "prompt_count": len(paired_items),
        "items": paired_items,
        "summary": {
            "prompt_count": len(paired_items),
            "base_average_latency_ms": base_result["summary"]["average_latency_ms"],
            "adapter_average_latency_ms": adapter_result["summary"]["average_latency_ms"],
            "base_exact_match_rate": base_result["summary"]["exact_match_rate"],
            "adapter_exact_match_rate": adapter_result["summary"]["exact_match_rate"],
            "base_contains_reference_rate": base_result["summary"]["contains_reference_rate"],
            "adapter_contains_reference_rate": adapter_result["summary"]["contains_reference_rate"],
            "improved_contains_reference_count": improved_contains_reference,
            "comparable_reference_items": comparable_items,
        },
    }


def main() -> int:
    raw = sys.stdin.read()
    if not raw.strip():
        return fail("invalid_input", "Worker input is required.")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as error:
        return fail("invalid_json", f"Worker input is not valid JSON: {error}")
    if not isinstance(payload, dict):
        return fail("invalid_input", "Worker input must be a JSON object.")

    model_path = str(payload.get("model_path", "")).strip()
    worker_python = str(payload.get("mlx_worker_python", "python3")).strip() or "python3"
    worker_path = str(payload.get("mlx_worker_path", "")).strip()
    if not model_path:
        return fail("invalid_input", "model_path is required.")
    if not worker_path:
        return fail("invalid_input", "mlx_worker_path is required.")

    prompt_set = []
    try:
        prompt_set = normalize_prompt_set(payload.get("prompt_set"))
    except Exception as error:
        return fail("invalid_prompt_set", str(error))

    op = str(payload.get("op", "")).strip().lower()
    if op not in {"prompt_eval", "adapter_eval"}:
        return fail("invalid_op", "op must be prompt_eval or adapter_eval.")
    adapter_path = str(payload.get("adapter_path", "")).strip()
    if op == "adapter_eval" and not adapter_path:
        return fail("invalid_input", "adapter_path is required for adapter_eval.")

    generation = payload.get("generation") if isinstance(payload.get("generation"), dict) else {}
    generation_payload = {
        "temperature": float(generation.get("temperature", 0.2)),
        "top_p": float(generation.get("top_p", 0.95)),
        "top_k": int(generation.get("top_k", 50)),
        "max_tokens": int(generation.get("max_tokens", 512)),
        "repetition_penalty": float(generation.get("repetition_penalty", 1.0)),
        "seed": generation.get("seed"),
        "enable_thinking": bool(generation.get("enable_thinking", False)),
    }
    system_prompt = str(payload.get("system_prompt", "") or "").strip()
    max_context_chars = max(2000, min(int(payload.get("max_context_chars", 56000) or 56000), 56000))
    contract = build_contract(max_context_chars)

    command = [
        worker_python,
        str(Path(worker_path).expanduser()),
        "--model-path",
        str(Path(model_path).expanduser()),
        "--max-context-chars",
        str(max_context_chars),
    ]

    process: subprocess.Popen[str] | None = None
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        startup = read_worker_response(process, "startup", 120)
        if not bool(startup.get("ok")):
            error = startup.get("error") if isinstance(startup.get("error"), dict) else {}
            raise RuntimeError(str(error.get("message", "MLX worker startup failed.")))
        if op == "prompt_eval":
            result = run_prompt_eval(
                process,
                prompt_set=prompt_set,
                contract=contract,
                generation=generation_payload,
                system_prompt=system_prompt,
            )
        else:
            result = run_adapter_eval(
                process,
                prompt_set=prompt_set,
                contract=contract,
                generation=generation_payload,
                system_prompt=system_prompt,
                adapter_path=adapter_path,
            )
        return emit({"ok": True, "data": result, "created_at": now_iso()})
    except Exception as error:
        return fail("worker_failed", str(error))
    finally:
        if process is not None:
            try:
                rpc(process, op="shutdown", payload={}, timeout_sec=3)
            except Exception:
                pass
            try:
                process.terminate()
            except Exception:
                pass
            try:
                process.wait(timeout=2)
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass


if __name__ == "__main__":
    raise SystemExit(main())

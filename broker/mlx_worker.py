#!/usr/bin/env python3
from __future__ import annotations

import argparse
import inspect
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CONTRACT_BASE = {
    "schema_version": "mlx_chat_v1",
    "message_format": "openai_chat_messages_v1",
    "tool_call_format": "none_v1",
    "chat_template_assumption": "qwen_jinja_default_or_plaintext_fallback_v1",
    "tokenizer_template_mode": "apply_chat_template_default_v1",
    "max_context_behavior": "tail_truncate_chars_v1",
}


def build_contract(max_context_chars: int) -> dict[str, Any]:
    return {
        **CONTRACT_BASE,
        "max_context_chars": int(max_context_chars),
    }


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=True) + "\n")
    sys.stdout.flush()


def fail(
    request_id: str,
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> None:
    emit(
        {
            "request_id": request_id,
            "ok": False,
            "error": {
                "code": code,
                "message": message,
                "details": details or {},
            },
            "created_at": now_iso(),
        }
    )


def sanitize_messages(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise ValueError("messages must be an array.")
    normalized: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role", "")).strip()
        if role not in {"system", "user", "assistant"}:
            continue
        content = str(item.get("content", ""))
        normalized.append({"role": role, "content": content})
    if not normalized:
        raise ValueError("messages must include at least one valid chat message.")
    return normalized


def assert_contract(contract: Any, expected: dict[str, Any]) -> None:
    if not isinstance(contract, dict):
        raise ValueError("contract is required and must be an object.")
    for key, expected_value in expected.items():
        actual_value = contract.get(key)
        if actual_value != expected_value:
            raise ValueError(
                f"contract mismatch for '{key}': expected '{expected_value}', got '{actual_value}'."
            )


def build_prompt(messages: list[dict[str, str]], max_context_chars: int) -> str:
    # Keep an explicit, deterministic fallback for environments where chat templating is unavailable.
    parts = []
    for message in messages:
        role = message["role"].upper()
        content = str(message.get("content", "")).strip()
        if not content:
            continue
        parts.append(f"{role}:\n{content}")
    merged = "\n\n".join(parts).strip()
    if not merged:
        raise ValueError("Prompt is empty after normalization.")
    last_role = next(
        (message["role"] for message in reversed(messages) if str(message.get("content", "")).strip()),
        "",
    )
    assistant_cue = "\n\nASSISTANT:\n" if last_role != "assistant" else ""
    if max_context_chars > 0:
        history_budget = max(1, max_context_chars - len(assistant_cue))
        if len(merged) > history_budget:
            merged = merged[-history_budget:]
    if assistant_cue:
        merged = f"{merged}{assistant_cue}"
    return merged


def _truncate_prompt_tail(text: str, max_context_chars: int) -> str:
    if max_context_chars <= 0:
        return text
    return text if len(text) <= max_context_chars else text[-max_context_chars:]


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def apply_default_chat_template(
    tokenizer: Any,
    messages: list[dict[str, str]],
    *,
    enable_thinking: bool = False,
) -> str:
    template_fn = getattr(tokenizer, "apply_chat_template", None)
    if not callable(template_fn):
        return ""
    try:
        signature = inspect.signature(template_fn)
        template_kwargs: dict[str, Any] = {}
        if "messages" in signature.parameters:
            template_kwargs["messages"] = messages
        elif "conversation" in signature.parameters:
            template_kwargs["conversation"] = messages
        else:
            raise TypeError("tokenizer.apply_chat_template does not accept messages or conversation keyword.")
        if "add_generation_prompt" in signature.parameters:
            template_kwargs["add_generation_prompt"] = True
        if "enable_thinking" in signature.parameters:
            template_kwargs["enable_thinking"] = bool(enable_thinking)
        if "tokenize" in signature.parameters:
            template_kwargs["tokenize"] = False
        if "padding" in signature.parameters:
            template_kwargs["padding"] = False
        prompt = template_fn(**template_kwargs)
    except TypeError:
        # Some mlx/tokenizer builds only accept positional messages or a subset of kwargs.
        try:
            prompt = template_fn(messages, add_generation_prompt=True, enable_thinking=bool(enable_thinking))
            if not prompt:
                return ""
            if isinstance(prompt, (list, tuple)) and hasattr(tokenizer, "decode"):
                decoded = tokenizer.decode(prompt)  # type: ignore[call-arg]
                prompt = decoded
            if not isinstance(prompt, str):
                prompt = str(prompt)
            return prompt.strip()
        except Exception:
            pass
        try:
            prompt = template_fn(messages)  # type: ignore[misc]
        except Exception:
            return ""
    except Exception:
        return ""

    if not prompt:
        return ""
    if isinstance(prompt, (list, tuple)) and hasattr(tokenizer, "decode"):
        decoded = tokenizer.decode(prompt)  # type: ignore[call-arg]
        return str(decoded or "").strip()
    return str(prompt).strip()


def parse_text_output(raw: Any) -> str:
    if isinstance(raw, str):
        return raw
    if isinstance(raw, dict):
        for key in ("text", "output", "response"):
            if key in raw and isinstance(raw[key], str):
                return raw[key]
    if isinstance(raw, (list, tuple)) and raw:
        for item in raw:
            text = parse_text_output(item)
            if text:
                return text
    return str(raw or "")


@dataclass
class GenerationResult:
    text: str
    token_count: int
    latency_ms: int


class MlxEngine:
    def __init__(self, model_path: str, max_context_chars: int) -> None:
        self.model_path = model_path
        self.max_context_chars = max(2000, int(max_context_chars))
        self._generate_fn = None
        self._stream_generate_fn = None
        self._load_fn = None
        self._make_sampler_fn = None
        self._make_logits_processors_fn = None
        self._generate_signature: inspect.Signature | None = None
        self.model: Any = None
        self.tokenizer: Any = None
        self.active_adapter_path = ""
        self._load_runtime()
        self._load_model(adapter_path=None)

    def _load_runtime(self) -> None:
        try:
            from mlx_lm import generate as mlx_generate  # type: ignore
            from mlx_lm import load as mlx_load  # type: ignore
        except Exception as error:
            raise RuntimeError(
                "mlx_lm is not installed or failed to import. Install mlx-lm in the worker environment."
            ) from error
        self._generate_fn = mlx_generate
        try:
            from mlx_lm import stream_generate as mlx_stream_generate  # type: ignore
            self._stream_generate_fn = mlx_stream_generate
        except Exception:
            self._stream_generate_fn = None
        self._load_fn = mlx_load
        try:
            from mlx_lm.sample_utils import make_logits_processors  # type: ignore
            from mlx_lm.sample_utils import make_sampler  # type: ignore
            self._make_sampler_fn = make_sampler
            self._make_logits_processors_fn = make_logits_processors
        except Exception:
            self._make_sampler_fn = None
            self._make_logits_processors_fn = None
        try:
            self._generate_signature = inspect.signature(mlx_generate)
        except Exception:
            self._generate_signature = None

    def _load_model(self, adapter_path: str | None) -> None:
        assert self._load_fn is not None
        kwargs: dict[str, Any] = {}
        if adapter_path:
            kwargs["adapter_path"] = adapter_path
        try:
            model, tokenizer = self._load_fn(self.model_path, **kwargs)
        except TypeError:
            model, tokenizer = self._load_fn(self.model_path)
            if adapter_path:
                self._apply_adapter(model, adapter_path)
        self.model = model
        self.tokenizer = tokenizer
        self.active_adapter_path = adapter_path or ""

    def _apply_adapter(self, model: Any, adapter_path: str) -> None:
        try:
            from mlx_lm.tuner.utils import load_adapters  # type: ignore
        except Exception as error:
            raise RuntimeError(
                "Current mlx_lm build does not expose adapter loading helpers."
            ) from error
        load_adapters(model, adapter_path)

    def set_adapter(self, adapter_path: str | None) -> None:
        self._load_model(adapter_path=adapter_path or None)

    def generate(self, messages: list[dict[str, str]], params: dict[str, Any]) -> GenerationResult:
        if self.model is None or self.tokenizer is None or self._generate_fn is None:
            raise RuntimeError("MLX runtime is not initialized.")
        prompt = self._build_prompt(messages, self.max_context_chars, params)
        kwargs = self._map_generate_kwargs(params)
        started = time.monotonic()
        raw = self._generate_with_compat(prompt, params, kwargs)
        text = parse_text_output(raw).strip()
        latency_ms = int((time.monotonic() - started) * 1000)
        token_count = max(0, len(text.split()))
        return GenerationResult(
            text=text,
            token_count=token_count,
            latency_ms=latency_ms,
        )

    def generate_stream(
        self,
        messages: list[dict[str, str]],
        params: dict[str, Any],
        on_delta: Any = None,
    ) -> GenerationResult:
        if self.model is None or self.tokenizer is None:
            raise RuntimeError("MLX runtime is not initialized.")
        prompt = self._build_prompt(messages, self.max_context_chars, params)
        started = time.monotonic()
        accumulated = ""

        if callable(self._stream_generate_fn):
            kwargs = self._map_generate_kwargs(params)
            stream_fn = self._stream_generate_fn
            assert stream_fn is not None
            stream_iter = stream_fn(self.model, self.tokenizer, prompt=prompt, **kwargs)
            for chunk in stream_iter:
                text = ""
                if isinstance(chunk, str):
                    text = chunk
                elif hasattr(chunk, "text"):
                    text = str(getattr(chunk, "text") or "")
                elif isinstance(chunk, dict):
                    text = str(chunk.get("text", "") or chunk.get("token", "") or "")
                else:
                    text = str(chunk or "")
                if not text:
                    continue
                accumulated += text
                if on_delta:
                    on_delta(text, accumulated)
        else:
            # Fallback: preserve compatibility on older mlx_lm builds that do not expose streaming.
            generated = self.generate(messages, params)
            accumulated = generated.text
            if accumulated and on_delta:
                on_delta(accumulated, accumulated)

        latency_ms = int((time.monotonic() - started) * 1000)
        token_count = max(0, len(accumulated.split()))
        return GenerationResult(
            text=accumulated.strip(),
            token_count=token_count,
            latency_ms=latency_ms,
        )

    def _build_prompt(self, messages: list[dict[str, str]], max_context_chars: int, params: dict[str, Any]) -> str:
        prompt = ""
        enable_thinking = _coerce_bool(params.get("enable_thinking"), default=False)
        if self.tokenizer is not None:
            prompt = apply_default_chat_template(
                self.tokenizer,
                messages,
                enable_thinking=enable_thinking,
            )
        if not prompt:
            prompt = build_prompt(messages, max_context_chars)
            return prompt
        return _truncate_prompt_tail(prompt, max_context_chars)

    def _map_generate_kwargs(self, params: dict[str, Any]) -> dict[str, Any]:
        mapped: dict[str, Any] = {}
        signature = self._generate_signature
        if signature is None:
            return mapped
        parameters = signature.parameters
        accepted = set(parameters.keys())
        accepts_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )

        def assign(target: str, value: Any) -> None:
            if accepts_kwargs or target in accepted:
                mapped[target] = value

        temperature = params.get("temperature")
        top_p = params.get("top_p")
        top_k = params.get("top_k")
        if self._make_sampler_fn is not None:
            temp_value = float(temperature) if temperature is not None else 0.0
            top_p_value = float(top_p) if top_p is not None else 0.0
            top_k_value = int(top_k) if top_k is not None else 0
            assign(
                "sampler",
                self._make_sampler_fn(
                    temp=temp_value,
                    top_p=top_p_value,
                    top_k=top_k_value,
                ),
            )
        elif temperature is not None:
            if accepts_kwargs or "temperature" in accepted:
                mapped["temperature"] = float(temperature)
            elif accepts_kwargs or "temp" in accepted:
                mapped["temp"] = float(temperature)
        if params.get("max_tokens") is not None:
            assign("max_tokens", int(params["max_tokens"]))
        if self._make_sampler_fn is None:
            if top_p is not None:
                assign("top_p", float(top_p))
            if top_k is not None:
                assign("top_k", int(top_k))
        repetition_penalty = params.get("repetition_penalty")
        if self._make_logits_processors_fn is not None and repetition_penalty is not None:
            processors = self._make_logits_processors_fn(
                repetition_penalty=float(repetition_penalty),
            )
            if processors:
                assign("logits_processors", processors)
        elif repetition_penalty is not None:
            assign("repetition_penalty", float(repetition_penalty))
        if params.get("seed") is not None:
            assign("seed", int(params["seed"]))
        # Only pass this flag when the runtime explicitly declares support.
        # Some mlx_lm versions accept **kwargs but still fail in deeper calls.
        if "enable_thinking" in accepted:
            mapped["enable_thinking"] = _coerce_bool(params.get("enable_thinking"), default=False)
        return mapped

    def _generate_with_compat(self, prompt: str, params: dict[str, Any], kwargs: dict[str, Any]) -> Any:
        assert self._generate_fn is not None
        active_kwargs = dict(kwargs)
        while True:
            try:
                return self._generate_fn(self.model, self.tokenizer, prompt=prompt, **active_kwargs)
            except TypeError as error:
                message = str(error)
                match = re.search(r"unexpected keyword argument '([^']+)'", message)
                if not match:
                    raise
                bad_key = match.group(1)
                if bad_key not in active_kwargs:
                    raise
                active_kwargs.pop(bad_key, None)

                # Temperature naming differs across mlx_lm versions.
                if bad_key == "temperature" and "temp" not in active_kwargs:
                    temperature = params.get("temperature")
                    if temperature is not None:
                        active_kwargs["temp"] = float(temperature)
                        continue
                if bad_key == "temp" and "temperature" not in active_kwargs:
                    temperature = params.get("temperature")
                    if temperature is not None:
                        active_kwargs["temperature"] = float(temperature)
                        continue


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local MLX worker for broker-managed chat inference.")
    parser.add_argument("--model-path", required=True, help="Path to MLX model directory.")
    parser.add_argument(
        "--max-context-chars",
        type=int,
        default=24000,
        help="Maximum characters retained after prompt normalization.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    model_path = Path(args.model_path).expanduser()
    if not model_path.exists():
        emit(
            {
                "request_id": "startup",
                "ok": False,
                "error": {
                    "code": "model_path_missing",
                    "message": f"Model path does not exist: {model_path}",
                    "details": {"model_path": str(model_path)},
                },
                "created_at": now_iso(),
            }
        )
        return 2

    try:
        engine = MlxEngine(str(model_path), int(args.max_context_chars))
    except Exception as error:
        emit(
            {
                "request_id": "startup",
                "ok": False,
                "error": {
                    "code": "startup_failed",
                    "message": str(error),
                    "details": {"model_path": str(model_path)},
                },
                "created_at": now_iso(),
            }
        )
        return 3

    worker_contract = build_contract(engine.max_context_chars)
    emit(
        {
            "request_id": "startup",
            "ok": True,
            "data": {
                "status": "ready",
                "model_path": str(model_path),
                "contract": worker_contract,
            },
            "created_at": now_iso(),
        }
    )

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        request_id = ""
        try:
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError("payload must be an object.")
            request_id = str(payload.get("request_id") or f"req_{int(time.time() * 1000)}")
            op = str(payload.get("op", "")).strip()
            if not op:
                raise ValueError("op is required.")

            if op == "health":
                emit(
                    {
                        "request_id": request_id,
                        "ok": True,
                        "data": {
                            "status": "ready",
                            "model_path": str(model_path),
                            "active_adapter_path": engine.active_adapter_path,
                            "contract": worker_contract,
                        },
                        "created_at": now_iso(),
                    }
                )
                continue

            if op == "shutdown":
                emit(
                    {
                        "request_id": request_id,
                        "ok": True,
                        "data": {"status": "stopping"},
                        "created_at": now_iso(),
                    }
                )
                return 0

            if op == "adapter_load":
                adapter_path = str(payload.get("adapter_path", "")).strip()
                if not adapter_path:
                    raise ValueError("adapter_path is required.")
                resolved = str(Path(adapter_path).expanduser())
                engine.set_adapter(resolved)
                emit(
                    {
                        "request_id": request_id,
                        "ok": True,
                        "data": {"active_adapter_path": resolved},
                        "created_at": now_iso(),
                    }
                )
                continue

            if op == "adapter_unload":
                engine.set_adapter(None)
                emit(
                    {
                        "request_id": request_id,
                        "ok": True,
                        "data": {"active_adapter_path": ""},
                        "created_at": now_iso(),
                    }
                )
                continue

            if op == "generate":
                schema_version = str(payload.get("schema_version", "")).strip()
                if schema_version != worker_contract["schema_version"]:
                    raise ValueError("schema_version mismatch.")
                assert_contract(payload.get("contract"), worker_contract)
                messages = sanitize_messages(payload.get("messages"))
                params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
                generated = engine.generate(messages, params)
                emit(
                    {
                        "request_id": request_id,
                        "ok": True,
                        "data": {
                            "text": generated.text,
                            "token_count": generated.token_count,
                            "latency_ms": generated.latency_ms,
                            "contract": worker_contract,
                        },
                        "created_at": now_iso(),
                    }
                )
                continue

            if op == "generate_stream":
                schema_version = str(payload.get("schema_version", "")).strip()
                if schema_version != worker_contract["schema_version"]:
                    raise ValueError("schema_version mismatch.")
                assert_contract(payload.get("contract"), worker_contract)
                messages = sanitize_messages(payload.get("messages"))
                params = payload.get("params") if isinstance(payload.get("params"), dict) else {}

                def _emit_delta(delta: str, text: str) -> None:
                    emit(
                        {
                            "request_id": request_id,
                            "event": "delta",
                            "ok": True,
                            "data": {"delta": delta, "text": text},
                            "created_at": now_iso(),
                        }
                    )

                generated = engine.generate_stream(messages, params, on_delta=_emit_delta)
                emit(
                    {
                        "request_id": request_id,
                        "event": "completed",
                        "ok": True,
                        "data": {
                            "text": generated.text,
                            "token_count": generated.token_count,
                            "latency_ms": generated.latency_ms,
                            "contract": worker_contract,
                        },
                        "created_at": now_iso(),
                    }
                )
                continue

            raise ValueError(f"Unsupported op: {op}")
        except Exception as error:
            fail(
                request_id or "unknown",
                "worker_error",
                str(error),
                details={"type": error.__class__.__name__},
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

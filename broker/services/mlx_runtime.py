from __future__ import annotations

import json
import select
import subprocess
import threading
import time
from datetime import datetime, timezone
import uuid
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from broker.local_broker import BrokerConfig

MLX_MAX_CONTEXT_CHARS_CAP = 56000


def _lb():
    from broker import local_broker

    return local_broker


def _coerce_optional_float(value: Any) -> float | None:
    if value in {"", None}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


MLX_CHAT_CONTRACT_BASE = {
    "schema_version": "mlx_chat_v1",
    "message_format": "openai_chat_messages_v1",
    "tool_call_format": "none_v1",
    "chat_template_assumption": "qwen_jinja_default_or_plaintext_fallback_v1",
    "tokenizer_template_mode": "apply_chat_template_default_v1",
    "max_context_behavior": "tail_truncate_chars_v1",
}
TRAINING_DATASET_MESSAGE_ROLES = {"system", "user", "assistant"}
TRAINING_BALANCED_PROFILE = {
    "rank": 8,
    "scale": 20.0,
    "dropout": 0.0,
    "num_layers": 8,
    "learning_rate": 1e-5,
    "iters": 600,
    "batch_size": 1,
    "grad_accumulation_steps": 4,
    "steps_per_report": 10,
    "steps_per_eval": 100,
    "save_every": 100,
    "val_batches": 25,
    "max_seq_length": 2048,
    "grad_checkpoint": True,
    "seed": 0,
}
TRAINING_PERIODIC_CHECKPOINT_LIMIT = 5


class BrowserConfigManager:
    def __init__(self, data_dir: Path) -> None:
        self._lock = threading.Lock()
        self._config_path = data_dir / "browser_config.json"
        self._agent_max_steps = BROWSER_AGENT_MAX_STEPS_DEFAULT
        self._load_persisted_config()

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        tmp.replace(path)

    def _load_json(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _normalize_agent_max_steps(self, value: Any) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError) as error:
            raise ValueError("agent_max_steps must be an integer.") from error
        if parsed < BROWSER_AGENT_MAX_STEPS_MIN or parsed > BROWSER_AGENT_MAX_STEPS_MAX:
            raise ValueError(
                f"agent_max_steps must be between {BROWSER_AGENT_MAX_STEPS_MIN} and {BROWSER_AGENT_MAX_STEPS_MAX}."
            )
        return parsed

    def _config_payload_locked(self) -> dict[str, Any]:
        return {
            "agent_max_steps": self._agent_max_steps,
            "limits": {
                "agent_max_steps": {
                    "min": BROWSER_AGENT_MAX_STEPS_MIN,
                    "max": BROWSER_AGENT_MAX_STEPS_MAX,
                }
            },
        }

    def _load_persisted_config(self) -> None:
        payload = self._load_json(self._config_path)
        raw_steps = payload.get("agent_max_steps", payload.get("agentMaxSteps"))
        if raw_steps is None:
            return
        try:
            self._agent_max_steps = self._normalize_agent_max_steps(raw_steps)
        except ValueError:
            self._agent_max_steps = BROWSER_AGENT_MAX_STEPS_DEFAULT

    def _save_persisted_config_locked(self) -> None:
        self._write_json(
            self._config_path,
            {
                "agent_max_steps": self._agent_max_steps,
            },
        )

    def config(self) -> dict[str, Any]:
        with self._lock:
            return self._config_payload_locked()

    def agent_max_steps(self) -> int:
        with self._lock:
            return self._agent_max_steps

    def update_config(self, updates: dict[str, Any]) -> dict[str, Any]:
        raw_steps = updates.get("agent_max_steps", updates.get("agentMaxSteps"))
        with self._lock:
            if raw_steps is not None:
                self._agent_max_steps = self._normalize_agent_max_steps(raw_steps)
                self._save_persisted_config_locked()
            return self._config_payload_locked()

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

def read_mlx_worker_response(
    process: subprocess.Popen[str],
    expected_request_id: str,
    timeout_sec: float,
) -> dict[str, Any]:

    def _stderr_excerpt() -> str:
        try:
            if not process.stderr:
                return ""
            return summarize_mlx_worker_failure(process.stderr.read() or "")
        except Exception:
            return ""

    deadline = time.monotonic() + max(0.1, timeout_sec)
    fd = process.stdout.fileno()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            if process.poll() is not None:
                detail = _stderr_excerpt()
                if detail:
                    raise RuntimeError(f"MLX worker exited before responding: {detail}")
            raise TimeoutError("Timed out waiting for MLX worker response.")
        ready, _, _ = select.select([fd], [], [], remaining)
        if not ready:
            if process.poll() is not None:
                detail = _stderr_excerpt()
                if detail:
                    raise RuntimeError(f"MLX worker exited before responding: {detail}")
            continue
        line = process.stdout.readline()
        if line == "":
            detail = _stderr_excerpt()
            if detail:
                raise RuntimeError(f"MLX worker closed its stdout stream: {detail}")
            raise RuntimeError("MLX worker closed its stdout stream.")
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, dict):
            continue
        if str(parsed.get("request_id", "")) == expected_request_id:
            return parsed

def run_ephemeral_mlx_completion(
    messages: list[dict[str, str]],
    *,
    cancel_check: Any = None,
) -> str:
    if cancel_check and cancel_check():
        raise _lb().RouteRequestCancelledError("Request cancelled by user.")
    if not _lb().CONFIG.mlx_model_path:
        raise RuntimeError("MLX is not configured. Set BROKER_MLX_MODEL_PATH first.")
    if not _lb().CONFIG.mlx_worker_path.exists():
        raise RuntimeError(f"MLX worker script not found: {_lb().CONFIG.mlx_worker_path}")
    contract = {
        **MLX_CHAT_CONTRACT_BASE,
        "max_context_chars": _lb().MLX_RUNTIME.effective_max_context_chars(),
    }
    command = [
        _lb().CONFIG.mlx_worker_python,
        str(_lb().CONFIG.mlx_worker_path),
        "--model-path",
        str(Path(_lb().CONFIG.mlx_model_path).expanduser()),
        "--max-context-chars",
        str(contract["max_context_chars"]),
    ]
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    try:
        startup = read_mlx_worker_response(process, "startup", float(_lb().CONFIG.mlx_start_timeout_sec))
        if not bool(startup.get("ok")):
            error = startup.get("error") if isinstance(startup.get("error"), dict) else {}
            raise RuntimeError(str(error.get("message", "MLX worker startup failed.")))
        request_id = f"mlx_{uuid.uuid4().hex[:12]}"
        payload = {
            "request_id": request_id,
            "op": "generate",
            "schema_version": contract["schema_version"],
            "contract": contract,
            "messages": messages,
            "params": _lb().MLX_RUNTIME.status().get("generation_config", {}),
        }
        process.stdin.write(json.dumps(payload, ensure_ascii=True) + "\n")
        process.stdin.flush()
        if cancel_check and cancel_check():
            raise _lb().RouteRequestCancelledError("Request cancelled by user.")
        response = read_mlx_worker_response(process, request_id, float(_lb().CONFIG.mlx_generation_timeout_sec))
        if not bool(response.get("ok")):
            error = response.get("error") if isinstance(response.get("error"), dict) else {}
            raise RuntimeError(str(error.get("message", "MLX paper analysis failed.")))
        data = response.get("data") if isinstance(response.get("data"), dict) else {}
        return str(data.get("text", "")).strip()
    finally:
        try:
            shutdown_id = f"mlx_{uuid.uuid4().hex[:12]}"
            if process.stdin and process.poll() is None:
                process.stdin.write(json.dumps({"request_id": shutdown_id, "op": "shutdown"}, ensure_ascii=True) + "\n")
                process.stdin.flush()
        except Exception:
            pass
        _lb().terminate_subprocess(process, timeout_sec=float(_lb().CONFIG.mlx_stop_timeout_sec))

class MlxRuntimeManager:
    def __init__(self, config: BrokerConfig) -> None:
        self._config = config
        self._lock = threading.Lock()
        self._process: subprocess.Popen[str] | None = None
        self._status = "disabled" if not str(config.mlx_url or "").strip() else "ready"
        self._last_error = ""
        self._started_at = ""
        self._restart_success_count = 0
        self._restart_failure_count = 0
        self._telemetry: deque[dict[str, Any]] = deque(maxlen=120)

        self._url = str(config.mlx_url or "").strip()
        self._model = str(config.mlx_model or "").strip()
        self._api_key = str(config.mlx_api_key or "").strip()

        self._model_path = ""
        self._worker_path = Path()
        self._worker_python = str(config.mlx_worker_python or "python3")

        self._config_path = config.data_dir / "mlx_config.json"
        self._adapters_path = config.data_dir / "mlx_adapters.json"
        self._adapters: list[dict[str, Any]] = []
        self._active_adapter_id = ""

        self._generation_config: dict[str, Any] = {}
        self._system_prompt = ""

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        tmp.replace(path)

    def _load_json(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _normalize_generation_config(self, value: Any) -> dict[str, Any]:
        raw = value if isinstance(value, dict) else {}
        seed_value = raw.get("seed")
        seed: int | None
        if seed_value in {"", None}:
            seed = None
        else:
            try:
                seed = int(seed_value)
            except (TypeError, ValueError):
                seed = None
        raw_enable_thinking = raw.get(
            "enable_thinking",
            raw.get("enableThinking", self._generation_config["enable_thinking"]),
        )
        if isinstance(raw_enable_thinking, bool):
            enable_thinking = raw_enable_thinking
        elif isinstance(raw_enable_thinking, (int, float)):
            enable_thinking = bool(raw_enable_thinking)
        else:
            enable_thinking = str(raw_enable_thinking).strip().lower() in {"1", "true", "yes", "on"}
        return {
            "temperature": float(raw.get("temperature", self._generation_config["temperature"])),
            "top_p": float(raw.get("top_p", self._generation_config["top_p"])),
            "top_k": int(raw.get("top_k", self._generation_config["top_k"])),
            "max_tokens": int(raw.get("max_tokens", self._generation_config["max_tokens"])),
            "repetition_penalty": float(
                raw.get("repetition_penalty", self._generation_config["repetition_penalty"])
            ),
            "seed": seed,
            "enable_thinking": enable_thinking,
        }

    def _normalize_system_prompt(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def _load_persisted_config(self) -> None:
        payload = self._load_json(self._config_path)
        normalized = self._normalize_generation_config(payload.get("generation", {}))
        self._generation_config.update(normalized)
        if "system_prompt" in payload or "systemPrompt" in payload:
            raw_prompt = payload.get("system_prompt", payload.get("systemPrompt", ""))
            self._system_prompt = self._normalize_system_prompt(raw_prompt)

    def _save_persisted_config(self) -> None:
        self._write_json(
            self._config_path,
            {
                "generation": self._generation_config,
                "system_prompt": self._system_prompt,
            },
        )

    def _normalize_adapter(self, value: Any) -> dict[str, Any] | None:
        if not isinstance(value, dict):
            return None
        adapter_id = str(value.get("id", "")).strip()
        adapter_path = str(value.get("path", "")).strip()
        if not adapter_id or not adapter_path:
            return None
        name = str(value.get("name", "")).strip() or Path(adapter_path).name
        created_at = str(value.get("created_at", "")).strip() or now_iso()
        return {
            "id": adapter_id,
            "name": name,
            "path": str(Path(adapter_path).expanduser()),
            "created_at": created_at,
            "source_type": str(value.get("source_type", "")).strip(),
            "run_id": str(value.get("run_id", "")).strip(),
            "checkpoint_kind": str(value.get("checkpoint_kind", "")).strip(),
            "step": int(value.get("step", 0) or 0),
            "validation_loss": _coerce_optional_float(value.get("validation_loss")),
            "dataset_id": str(value.get("dataset_id", "")).strip(),
            "promoted": bool(value.get("promoted", False)),
        }

    def _load_adapters(self) -> None:
        payload = self._load_json(self._adapters_path)
        loaded: list[dict[str, Any]] = []
        for entry in payload.get("adapters", []):
            normalized = self._normalize_adapter(entry)
            if normalized:
                loaded.append(normalized)
        self._adapters = loaded
        active_id = str(payload.get("active_adapter_id", "")).strip()
        if active_id and any(item["id"] == active_id for item in loaded):
            self._active_adapter_id = active_id
        else:
            self._active_adapter_id = ""

    def _save_adapters(self) -> None:
        self._write_json(
            self._adapters_path,
            {
                "adapters": self._adapters,
                "active_adapter_id": self._active_adapter_id,
            },
        )

    def is_available(self) -> bool:
        return bool(self._url)

    def _active_adapter_locked(self) -> dict[str, Any] | None:
        if not self._active_adapter_id:
            return None
        for adapter in self._adapters:
            if adapter["id"] == self._active_adapter_id:
                return dict(adapter)
        return None

    def _effective_max_context_chars_locked(self) -> int:
        return min(
            MLX_MAX_CONTEXT_CHARS_CAP,
            max(2000, int(self._config.mlx_max_context_chars)),
        )

    def effective_max_context_chars(self) -> int:
        with self._lock:
            return self._effective_max_context_chars_locked()

    def _contract_locked(self) -> dict[str, Any]:
        return {
            **MLX_CHAT_CONTRACT_BASE,
            "max_context_chars": self._effective_max_context_chars_locked(),
        }

    def _assert_worker_contract_locked(self, contract: Any) -> None:
        if not isinstance(contract, dict):
            raise RuntimeError("MLX worker contract is missing or invalid.")
        expected = self._contract_locked()
        for key, expected_value in expected.items():
            actual_value = contract.get(key)
            if actual_value != expected_value:
                raise RuntimeError(
                    f"MLX worker contract mismatch for '{key}': expected '{expected_value}', got '{actual_value}'."
                )

    def _status_payload_locked(self) -> dict[str, Any]:
        health = _lb().mlx_backend_health(self._config)
        latency_points = [int(item.get("latency_ms", 0)) for item in list(self._telemetry)[-30:]]
        tps_points = [float(item.get("tokens_per_sec", 0.0)) for item in list(self._telemetry)[-30:]]
        return {
            "available": bool(health.get("available")),
            "status": str(health.get("status") or "disabled"),
            "url": str(health.get("url") or ""),
            "model": str(health.get("model") or ""),
            "configured_model": str(health.get("configured_model") or ""),
            "advertised_models": list(health.get("advertised_models") or []),
            "model_source": str(health.get("model_source") or ""),
            "last_error": str(health.get("last_error") or ""),
            "capabilities": dict(health.get("capabilities") or {}),
            "model_path": str(health.get("url") or ""),
            "worker_path": "",
            "worker_pid": None,
            "started_at": "",
            "generation_config": {},
            "system_prompt": "",
            "active_adapter": None,
            "contract": {},
            "metrics": {
                "latency_ms": latency_points,
                "tokens_per_sec": tps_points,
                "restart_success_count": 0,
                "restart_failure_count": 0,
            },
        }

    def status(self) -> dict[str, Any]:
        with self._lock:
            return self._status_payload_locked()

    def models_payload(self) -> dict[str, Any]:
        with self._lock:
            return _lb().build_models_payload(mlx_payload=self._status_payload_locked())

    def _set_status_locked(self, status: str, error: str = "") -> None:
        self._status = status
        self._last_error = error
        if status == "running":
            self._started_at = now_iso()
        elif status in {"stopped", "failed", "disabled"}:
            self._started_at = ""

    def _stderr_excerpt_locked(self, process: subprocess.Popen[str]) -> str:
        try:
            if not process.stderr:
                return ""
            return summarize_mlx_worker_failure(process.stderr.read() or "")
        except Exception:
            return ""

    def _readline_with_timeout(
        self,
        process: subprocess.Popen[str],
        stream: Any,
        timeout_sec: float,
    ) -> str:
        if timeout_sec <= 0:
            timeout_sec = 0.1
        fd = stream.fileno()
        end_at = time.monotonic() + timeout_sec
        while True:
            remaining = end_at - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for MLX worker response.")
            ready, _, _ = select.select([fd], [], [], remaining)
            if not ready:
                if process.poll() is not None:
                    detail = self._stderr_excerpt_locked(process)
                    if detail:
                        raise RuntimeError(f"MLX worker exited before responding: {detail}")
                    raise RuntimeError("MLX worker exited before responding.")
                continue
            line = stream.readline()
            if line == "":
                detail = self._stderr_excerpt_locked(process)
                if detail:
                    raise RuntimeError(f"MLX worker closed its stdout stream: {detail}")
                raise RuntimeError("MLX worker closed its stdout stream.")
            return line.strip()

    def _read_response_locked(
        self,
        process: subprocess.Popen[str],
        expected_request_id: str,
        timeout_sec: float,
    ) -> dict[str, Any]:
        end_at = time.monotonic() + max(0.1, timeout_sec)
        while True:
            line = self._readline_with_timeout(process, process.stdout, max(0.1, end_at - time.monotonic()))
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(parsed, dict):
                continue
            request_id = str(parsed.get("request_id", ""))
            if request_id != expected_request_id:
                continue
            return parsed

    def _read_stream_response_locked(
        self,
        process: subprocess.Popen[str],
        expected_request_id: str,
        timeout_sec: float,
        on_event: Any = None,
        cancel_check: Any = None,
    ) -> dict[str, Any]:
        end_at = time.monotonic() + max(0.1, timeout_sec)
        while True:
            if cancel_check and cancel_check():
                raise _lb().RouteRequestCancelledError("Request cancelled by user.")
            line = self._readline_with_timeout(process, process.stdout, max(0.1, end_at - time.monotonic()))
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(parsed, dict):
                continue
            request_id = str(parsed.get("request_id", ""))
            if request_id != expected_request_id:
                continue
            event_type = str(parsed.get("event", "")).strip().lower()
            if event_type and event_type != "completed":
                if on_event:
                    on_event(parsed)
                continue
            return parsed

    def _rpc_locked(
        self,
        op: str,
        payload: dict[str, Any],
        *,
        timeout_sec: float,
    ) -> dict[str, Any]:
        process = self._process
        if not process or process.poll() is not None:
            self._set_status_locked("failed", "MLX worker process is not running.")
            raise RuntimeError("MLX worker process is not running.")
        request_id = f"mlx_{uuid.uuid4().hex[:12]}"
        request_payload = {"request_id": request_id, "op": op, **payload}
        process.stdin.write(json.dumps(request_payload, ensure_ascii=True) + "\n")
        process.stdin.flush()
        response = self._read_response_locked(process, request_id, timeout_sec)
        if not bool(response.get("ok")):
            error = response.get("error") if isinstance(response.get("error"), dict) else {}
            message = str(error.get("message", "")).strip() or "Unknown MLX worker error."
            raise RuntimeError(message)
        data = response.get("data")
        return data if isinstance(data, dict) else {}

    def start(self) -> dict[str, Any]:
        with self._lock:
            if not self.is_available():
                self._set_status_locked("disabled", "BROKER_MLX_MODEL_PATH is not configured.")
                raise RuntimeError("MLX is not configured. Set BROKER_MLX_MODEL_PATH first.")
            if self._status == "running" and self._process and self._process.poll() is None:
                return self._status_payload_locked()
            if not self._worker_path.exists():
                self._set_status_locked("failed", f"MLX worker script not found: {self._worker_path}")
                raise RuntimeError(f"MLX worker script not found: {self._worker_path}")

            self._set_status_locked("starting", "")
            command = [
                self._worker_python,
                str(self._worker_path),
                "--model-path",
                self._model_path,
                "--max-context-chars",
                str(self._effective_max_context_chars_locked()),
            ]
            try:
                process = subprocess.Popen(
                    command,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1,
                )
            except Exception as error:
                self._set_status_locked("failed", f"Failed to launch MLX worker: {error}")
                raise RuntimeError(f"Failed to launch MLX worker: {error}") from error

            self._process = process
            try:
                startup = self._read_response_locked(
                    process,
                    "startup",
                    float(self._config.mlx_start_timeout_sec),
                )
            except Exception as error:
                _lb().terminate_subprocess(process, timeout_sec=float(self._config.mlx_stop_timeout_sec))
                self._process = None
                self._set_status_locked("failed", str(error))
                raise RuntimeError(f"MLX startup failed: {error}") from error

            if not bool(startup.get("ok")):
                error_obj = startup.get("error") if isinstance(startup.get("error"), dict) else {}
                message = str(error_obj.get("message", "MLX startup failed."))
                _lb().terminate_subprocess(process, timeout_sec=float(self._config.mlx_stop_timeout_sec))
                self._process = None
                self._set_status_locked("failed", message)
                raise RuntimeError(message)

            startup_data = startup.get("data") if isinstance(startup.get("data"), dict) else {}
            try:
                self._assert_worker_contract_locked(startup_data.get("contract"))
            except Exception as error:
                _lb().terminate_subprocess(process, timeout_sec=float(self._config.mlx_stop_timeout_sec))
                self._process = None
                self._set_status_locked("failed", str(error))
                raise RuntimeError(str(error)) from error
            self._set_status_locked("running", "")
            active_adapter = self._active_adapter_locked()
            try:
                if active_adapter:
                    self._rpc_locked(
                        "adapter_load",
                        {"adapter_path": str(active_adapter["path"])},
                        timeout_sec=float(self._config.mlx_start_timeout_sec),
                    )
            except Exception as error:
                _lb().terminate_subprocess(process, timeout_sec=float(self._config.mlx_stop_timeout_sec))
                self._process = None
                self._set_status_locked("failed", f"MLX adapter restore failed: {error}")
                raise RuntimeError(f"MLX adapter restore failed: {error}") from error
            return self._status_payload_locked()

    def stop(self) -> dict[str, Any]:
        with self._lock:
            process = self._process
            if not process:
                self._set_status_locked("stopped", "")
                return self._status_payload_locked()
            if process.poll() is None:
                try:
                    self._rpc_locked(
                        "shutdown",
                        {},
                        timeout_sec=min(3.0, float(self._config.mlx_stop_timeout_sec)),
                    )
                except Exception:
                    pass
            _lb().terminate_subprocess(process, timeout_sec=float(self._config.mlx_stop_timeout_sec))
            self._process = None
            self._set_status_locked("stopped", "")
            return self._status_payload_locked()

    def restart(self) -> dict[str, Any]:
        try:
            self.stop()
            payload = self.start()
            with self._lock:
                self._restart_success_count += 1
                payload = self._status_payload_locked()
            return payload
        except Exception:
            with self._lock:
                self._restart_failure_count += 1
            raise

    def update_generation_config(self, updates: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(updates, dict):
            raise ValueError("config must be an object.")
        with self._lock:
            current = dict(self._generation_config)
            system_prompt = self._system_prompt
            if "temperature" in updates:
                current["temperature"] = float(updates["temperature"])
            if "top_p" in updates:
                current["top_p"] = float(updates["top_p"])
            if "top_k" in updates:
                current["top_k"] = int(updates["top_k"])
            if "max_tokens" in updates:
                current["max_tokens"] = int(updates["max_tokens"])
            if "repetition_penalty" in updates:
                current["repetition_penalty"] = float(updates["repetition_penalty"])
            if "seed" in updates:
                seed_value = updates["seed"]
                if seed_value in {"", None}:
                    current["seed"] = None
                else:
                    current["seed"] = int(seed_value)
            if "enable_thinking" in updates:
                current["enable_thinking"] = _lb().ensure_boolean_flag(updates["enable_thinking"], "enable_thinking")
            elif "enableThinking" in updates:
                current["enable_thinking"] = _lb().ensure_boolean_flag(updates["enableThinking"], "enableThinking")
            if "system_prompt" in updates:
                system_prompt = self._normalize_system_prompt(updates["system_prompt"])
            elif "systemPrompt" in updates:
                system_prompt = self._normalize_system_prompt(updates["systemPrompt"])
            if current["top_p"] <= 0 or current["top_p"] > 1:
                raise ValueError("top_p must be > 0 and <= 1.")
            if current["top_k"] < 1:
                raise ValueError("top_k must be >= 1.")
            if current["max_tokens"] < 16:
                raise ValueError("max_tokens must be >= 16.")
            if current["temperature"] < 0:
                raise ValueError("temperature must be >= 0.")
            if current["repetition_penalty"] <= 0:
                raise ValueError("repetition_penalty must be > 0.")
            self._generation_config = current
            self._system_prompt = system_prompt
            self._save_persisted_config()
            return self._status_payload_locked()

    def _messages_with_system_prompt_locked(self, messages: list[dict[str, str]]) -> list[dict[str, str]]:
        output = list(messages)
        system_parts: list[str] = []
        if self._system_prompt:
            system_parts.append(self._system_prompt)
        if bool(self._generation_config.get("enable_thinking")):
            system_parts.append(_lb().MLX_THINKING_INSTRUCTIONS)
        if system_parts:
            output = [{"role": "system", "content": "\n\n".join(system_parts)}, *output]
        return output

    def list_adapters(self) -> dict[str, Any]:
        with self._lock:
            return {
                "adapters": [dict(item) for item in self._adapters],
                "active_adapter": self._active_adapter_locked(),
            }

    def register_adapter(
        self,
        *,
        path: str,
        name: str = "",
        adapter_id: str = "",
        metadata: dict[str, Any] | None = None,
        activate: bool = False,
    ) -> dict[str, Any]:
        with self._lock:
            adapter_path = str(Path(path).expanduser()) if path else ""
            if not adapter_path:
                raise ValueError("path is required.")
            if not Path(adapter_path).exists():
                raise ValueError(f"Adapter path does not exist: {adapter_path}")
            selected: dict[str, Any] | None = None
            if adapter_id:
                for item in self._adapters:
                    if item["id"] == adapter_id:
                        selected = item
                        break
            if not selected:
                for item in self._adapters:
                    if item["path"] == adapter_path:
                        selected = item
                        break
            if not selected:
                selected = {
                    "id": adapter_id.strip() or f"adp_{uuid.uuid4().hex[:10]}",
                    "name": name.strip() or Path(adapter_path).name,
                    "path": adapter_path,
                    "created_at": now_iso(),
                }
                self._adapters.append(selected)
            else:
                selected["path"] = adapter_path
                if name.strip():
                    selected["name"] = name.strip()
            for key, value in (metadata or {}).items():
                selected[key] = value
            normalized = self._normalize_adapter(selected)
            if not normalized:
                raise ValueError("Adapter metadata is invalid.")
            for index, item in enumerate(self._adapters):
                if item["id"] == normalized["id"]:
                    self._adapters[index] = normalized
                    break
            if activate:
                self._active_adapter_id = str(normalized["id"])
                if self._status == "running" and self._process and self._process.poll() is None:
                    self._rpc_locked(
                        "adapter_load",
                        {"adapter_path": str(normalized["path"])},
                        timeout_sec=float(self._config.mlx_generation_timeout_sec),
                    )
            self._save_adapters()
            return {
                "adapters": [dict(item) for item in self._adapters],
                "active_adapter": self._active_adapter_locked(),
                "adapter": dict(normalized),
            }

    def load_adapter(self, *, adapter_id: str = "", path: str = "", name: str = "") -> dict[str, Any]:
        with self._lock:
            selected: dict[str, Any] | None = None
            if adapter_id:
                for item in self._adapters:
                    if item["id"] == adapter_id:
                        selected = item
                        break
                if not selected:
                    raise ValueError("adapter_id was not found.")
            else:
                adapter_path = str(Path(path).expanduser()) if path else ""
                if not adapter_path:
                    raise ValueError("path is required when adapter_id is not provided.")
                if not Path(adapter_path).exists():
                    raise ValueError(f"Adapter path does not exist: {adapter_path}")
                for item in self._adapters:
                    if item["path"] == adapter_path:
                        selected = item
                        break
            if selected:
                self._active_adapter_id = str(selected["id"])
                if self._status == "running" and self._process and self._process.poll() is None:
                    self._rpc_locked(
                        "adapter_load",
                        {"adapter_path": str(selected["path"])},
                        timeout_sec=float(self._config.mlx_generation_timeout_sec),
                    )
                self._save_adapters()
                return {
                    "adapters": [dict(item) for item in self._adapters],
                    "active_adapter": dict(selected),
                }
        payload = self.register_adapter(path=path, name=name, activate=True)
        return {
            "adapters": payload.get("adapters", []),
            "active_adapter": payload.get("active_adapter"),
        }

    def unload_adapter(self) -> dict[str, Any]:
        with self._lock:
            self._active_adapter_id = ""
            if self._status == "running" and self._process and self._process.poll() is None:
                self._rpc_locked(
                    "adapter_unload",
                    {},
                    timeout_sec=float(self._config.mlx_generation_timeout_sec),
                )
            self._save_adapters()
            return {
                "adapters": [dict(item) for item in self._adapters],
                "active_adapter": None,
            }

    def generate(
        self,
        messages: list[dict[str, str]],
        *,
        cancel_check: Any = None,
    ) -> str:
        if cancel_check and cancel_check():
            raise _lb().RouteRequestCancelledError("Request cancelled by user.")
        answer_text, _reasoning_text = _lb().call_local_backend(
            messages,
            backend="mlx",
            cancel_check=cancel_check,
        )
        return answer_text

    def generate_stream(
        self,
        messages: list[dict[str, str]],
        *,
        cancel_check: Any = None,
        on_text_delta: Any = None,
    ) -> str:
        if cancel_check and cancel_check():
            raise _lb().RouteRequestCancelledError("Request cancelled by user.")
        accumulated_text = ""

        def _on_state_delta(visible: str, _reasoning: str) -> None:
            nonlocal accumulated_text
            if cancel_check and cancel_check():
                raise _lb().RouteRequestCancelledError("Request cancelled by user.")
            delta = visible[len(accumulated_text) :] if visible.startswith(accumulated_text) else visible
            accumulated_text = visible
            if delta and on_text_delta:
                on_text_delta(delta, visible)

        answer_text, _reasoning_text = _lb().call_local_backend_stream(
            messages,
            backend="mlx",
            cancel_check=cancel_check,
            on_state_delta=_on_state_delta,
        )
        return answer_text

    def health(self) -> dict[str, Any]:
        with self._lock:
            status = self._status_payload_locked()
            return {
                "available": status["available"],
                "status": status["status"],
                "last_error": status["last_error"],
                "url": status.get("url", ""),
                "model": status.get("model", ""),
            }

def _run_experiment_job(
    self: MlxRuntimeManager,
    worker_payload: dict[str, Any],
    *,
    config: "BrokerConfig",
    cancel_check: Any = None,
) -> dict[str, Any]:
    if not config.experiment_worker_path.exists():
        raise RuntimeError(f"Experiment worker script not found: {config.experiment_worker_path}")
    completed = _lb().run_subprocess_with_cancel(
        [config.experiment_worker_python, str(config.experiment_worker_path)],
        input_text=json.dumps(worker_payload, ensure_ascii=True),
        timeout_sec=float(config.experiment_job_timeout_sec),
        cancel_check=cancel_check,
    )
    stdout = str(completed.stdout or "").strip()
    stderr = str(completed.stderr or "").strip()
    if completed.returncode != 0 and not stdout:
        raise RuntimeError(stderr or "Experiment worker exited unsuccessfully.")
    parsed = json.loads(stdout or "{}")
    if not isinstance(parsed, dict) or not bool(parsed.get("ok")):
        error = parsed.get("error") if isinstance(parsed.get("error"), dict) else {}
        raise RuntimeError(str(error.get("message", stderr or "Experiment worker failed.")))
    result = parsed.get("data")
    if not isinstance(result, dict):
        raise RuntimeError("Experiment worker returned an invalid payload.")
    return result


def _run_training_job(
    self: MlxRuntimeManager,
    worker_payload: dict[str, Any],
    *,
    config: "BrokerConfig",
    cancel_check: Any = None,
    on_event: Any = None,
) -> dict[str, Any]:
    if not config.training_worker_path.exists():
        raise RuntimeError(f"Training worker script not found: {config.training_worker_path}")
    return _lb().stream_training_worker_events(
        [config.training_worker_python, str(config.training_worker_path)],
        input_payload=worker_payload,
        timeout_sec=float(config.training_job_timeout_sec),
        cancel_check=cancel_check,
        on_event=on_event,
    )


MlxRuntimeManager.run_experiment_job = _run_experiment_job
MlxRuntimeManager.run_training_job = _run_training_job


def handle_models_get(runtime: MlxRuntimeManager) -> dict[str, Any]:
    return runtime.models_payload()


def handle_mlx_status_get(runtime: MlxRuntimeManager) -> dict[str, Any]:
    return {"mlx": runtime.status()}


def handle_mlx_config_post(runtime: MlxRuntimeManager, data: dict[str, Any]) -> dict[str, Any]:
    updates: dict[str, Any]
    generation = data.get("generation") if isinstance(data, dict) else None
    if isinstance(generation, dict):
        updates = dict(generation)
    else:
        updates = dict(data) if isinstance(data, dict) else {}
    if isinstance(data, dict):
        if "system_prompt" in data:
            updates["system_prompt"] = data.get("system_prompt")
        elif "systemPrompt" in data:
            updates["system_prompt"] = data.get("systemPrompt")
    status = runtime.update_generation_config(updates if isinstance(updates, dict) else {})
    return {"ok": True, "mlx": status}


def handle_mlx_session_action(runtime: MlxRuntimeManager, action: str) -> dict[str, Any]:
    normalized = str(action or "").strip().lower()
    if normalized == "start":
        status = runtime.start()
    elif normalized == "stop":
        status = runtime.stop()
    elif normalized == "restart":
        status = runtime.restart()
    else:
        raise ValueError("Unsupported MLX session action.")
    return {"ok": True, "mlx": status}


def handle_mlx_adapters_list(runtime: MlxRuntimeManager) -> dict[str, Any]:
    payload = runtime.list_adapters()
    return {"ok": True, **payload}


def handle_mlx_adapters_load(runtime: MlxRuntimeManager, data: dict[str, Any]) -> dict[str, Any]:
    adapter_id = str(data.get("adapter_id", data.get("adapterId", ""))).strip()
    adapter_path = str(data.get("path", data.get("adapter_path", data.get("adapterPath", "")))).strip()
    name = str(data.get("name", "")).strip()
    payload = runtime.load_adapter(
        adapter_id=adapter_id,
        path=adapter_path,
        name=name,
    )
    return {"ok": True, **payload}


def handle_mlx_adapters_unload(runtime: MlxRuntimeManager, _data: dict[str, Any]) -> dict[str, Any]:
    payload = runtime.unload_adapter()
    return {"ok": True, **payload}


__all__ = [
    "MlxRuntimeManager",
    "handle_models_get",
    "handle_mlx_adapters_list",
    "handle_mlx_adapters_load",
    "handle_mlx_adapters_unload",
    "handle_mlx_config_post",
    "handle_mlx_session_action",
    "handle_mlx_status_get",
    "read_mlx_worker_response",
    "run_ephemeral_mlx_completion",
    "summarize_mlx_worker_failure",
]

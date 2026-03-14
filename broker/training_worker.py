#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import select
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STEP_PATTERN = re.compile(r"\b(?:iter(?:ation)?|step)\s*[:#]?\s*(\d+)(?:\s*/\s*(\d+))?", re.I)
TRAIN_LOSS_PATTERN = re.compile(r"\btrain(?:ing)?\s+loss\b[^0-9-]*([0-9.]+(?:e[-+]?\d+)?)", re.I)
VAL_LOSS_PATTERN = re.compile(r"\b(?:val|valid|validation)\s+loss\b[^0-9-]*([0-9.]+(?:e[-+]?\d+)?)", re.I)
SAVE_PATTERN = re.compile(r"\b(?:save|saving|saved)\b", re.I)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=True) + "\n")
    sys.stdout.flush()


def fail(message: str) -> int:
    emit({"event": "error", "message": message, "created_at": now_iso()})
    return 1


def yaml_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(str(value))


def yaml_dump(value: Any, indent: int = 0) -> list[str]:
    prefix = " " * indent
    if isinstance(value, dict):
        lines: list[str] = []
        for key, item in value.items():
            if isinstance(item, (dict, list)):
                lines.append(f"{prefix}{key}:")
                lines.extend(yaml_dump(item, indent + 2))
            else:
                lines.append(f"{prefix}{key}: {yaml_scalar(item)}")
        return lines
    if isinstance(value, list):
        lines = []
        for item in value:
            if isinstance(item, (dict, list)):
                lines.append(f"{prefix}-")
                lines.extend(yaml_dump(item, indent + 2))
            else:
                lines.append(f"{prefix}- {yaml_scalar(item)}")
        return lines
    return [f"{prefix}{yaml_scalar(value)}"]


def newest_adapter_weight(adapter_dir: Path) -> Path | None:
    candidates = sorted(
        list(adapter_dir.glob("*.safetensors"))
        + list(adapter_dir.glob("*.npz"))
        + list(adapter_dir.glob("*.pt"))
        + list(adapter_dir.glob("*.bin")),
        key=lambda path: path.stat().st_mtime if path.exists() else 0,
        reverse=True,
    )
    return candidates[0] if candidates else None


def write_adapter_config(adapter_dir: Path, config: dict[str, Any]) -> None:
    payload = {
        "fine_tune_type": "lora",
        "lora_layers": int(config["num_layers"]),
        "lora_parameters": {
            "rank": int(config["rank"]),
            "scale": float(config["scale"]),
            "dropout": float(config["dropout"]),
        },
    }
    (adapter_dir / "adapter_config.json").write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def copy_tree(source: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(source, destination)


def snapshot_adapter(
    trainer_adapter_dir: Path,
    checkpoints_dir: Path,
    training_config: dict[str, Any],
    *,
    step: int,
    kind: str,
    label: str,
    validation_loss: float | None = None,
) -> dict[str, Any] | None:
    if not trainer_adapter_dir.exists():
        return None
    weight_file = newest_adapter_weight(trainer_adapter_dir)
    if weight_file is None:
        return None
    if kind in {"best", "latest"}:
        destination = checkpoints_dir / kind
    else:
        destination = checkpoints_dir / f"step_{step:05d}"
    copy_tree(trainer_adapter_dir, destination)
    write_adapter_config(destination, training_config)
    checkpoint = {
        "id": f"ckpt_{kind}_{step:05d}" if kind not in {"best", "latest"} else f"ckpt_{kind}",
        "kind": kind,
        "label": label,
        "step": int(step),
        "path": str(destination),
        "validation_loss": validation_loss,
        "created_at": now_iso(),
        "promoted": False,
    }
    emit({"event": "checkpoint", "checkpoint": checkpoint, "message": f"{label} checkpoint saved."})
    return checkpoint


def build_config(
    *,
    model_path: str,
    dataset_dir: Path,
    trainer_adapter_dir: Path,
    training_config: dict[str, Any],
    resume_weight_name: str | None,
) -> dict[str, Any]:
    config = {
        "model": model_path,
        "train": True,
        "data": str(dataset_dir),
        "adapter_path": str(trainer_adapter_dir),
        "lora_layers": int(training_config["num_layers"]),
        "lora_parameters": {
            "rank": int(training_config["rank"]),
            "scale": float(training_config["scale"]),
            "dropout": float(training_config["dropout"]),
        },
        "learning_rate": float(training_config["learning_rate"]),
        "batch_size": int(training_config["batch_size"]),
        "iters": int(training_config["iters"]),
        "steps_per_report": int(training_config["steps_per_report"]),
        "steps_per_eval": int(training_config["steps_per_eval"]),
        "save_every": int(training_config["save_every"]),
        "val_batches": int(training_config["val_batches"]),
        "max_seq_length": int(training_config["max_seq_length"]),
        "grad_checkpoint": bool(training_config["grad_checkpoint"]),
        "grad_accumulation_steps": int(training_config["grad_accumulation_steps"]),
        "seed": int(training_config["seed"]),
        "test": (dataset_dir / "test.jsonl").exists(),
        "resume_adapter_file": resume_weight_name,
    }
    return config


def parse_line(line: str, state: dict[str, Any]) -> None:
    text = " ".join(str(line or "").split())
    if not text:
        return
    step_match = STEP_PATTERN.search(text)
    if step_match:
        state["current_step"] = max(state["current_step"], int(step_match.group(1)))
        if step_match.group(2):
            state["total_steps"] = max(state["total_steps"], int(step_match.group(2)))
    train_match = TRAIN_LOSS_PATTERN.search(text)
    if train_match:
        try:
            state["latest_train_loss"] = float(train_match.group(1))
        except ValueError:
            pass
    val_match = VAL_LOSS_PATTERN.search(text)
    if val_match:
        try:
            state["latest_validation_loss"] = float(val_match.group(1))
        except ValueError:
            pass
    state["status_message"] = text[:240]


def progress_payload(state: dict[str, Any], *, phase: str) -> dict[str, Any]:
    elapsed_sec = int(max(0, time.monotonic() - state["started_monotonic"]))
    current_step = int(state["current_step"])
    total_steps = max(1, int(state["total_steps"]))
    percent = round(min(100.0, (current_step / total_steps) * 100.0), 2)
    eta_sec: int | None = None
    if current_step > 0 and elapsed_sec > 0 and current_step < total_steps:
        eta_sec = int((elapsed_sec / current_step) * max(0, total_steps - current_step))
    return {
        "phase": phase,
        "percent": percent,
        "current_step": current_step,
        "total_steps": total_steps,
        "latest_train_loss": state["latest_train_loss"],
        "latest_validation_loss": state["latest_validation_loss"],
        "elapsed_sec": elapsed_sec,
        "eta_sec": eta_sec,
        "last_checkpoint_step": int(state["last_checkpoint_step"]),
        "last_checkpoint_kind": str(state["last_checkpoint_kind"]),
        "status_message": str(state["status_message"]),
    }


def prune_periodic_checkpoints(checkpoints_dir: Path, keep_last: int) -> None:
    periodic = sorted(
        [path for path in checkpoints_dir.glob("step_*") if path.is_dir()],
        key=lambda path: path.name,
    )
    if len(periodic) <= keep_last:
        return
    for path in periodic[:-keep_last]:
        shutil.rmtree(path, ignore_errors=True)


def trainer_command(payload: dict[str, Any], config_path: Path) -> list[str]:
    raw = payload.get("trainer_command")
    if isinstance(raw, list) and raw:
        return [str(item) for item in raw] + [str(config_path)]
    trainer_script = str(payload.get("trainer_script_path", "") or "").strip()
    trainer_python = str(payload.get("trainer_python", "python3") or "python3").strip()
    if trainer_script:
        return [trainer_python, trainer_script, str(config_path)]
    return [trainer_python, "-m", "mlx_lm.lora", "--config", str(config_path)]


def main() -> int:
    raw = sys.stdin.read()
    if not raw.strip():
        return fail("Training worker input is required.")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as error:
        return fail(f"Training worker input is not valid JSON: {error}")
    if not isinstance(payload, dict):
        return fail("Training worker input must be a JSON object.")

    model_path = str(payload.get("model_path", "")).strip()
    dataset_dir = Path(str(payload.get("dataset_dir", "")).strip()).expanduser()
    run_dir = Path(str(payload.get("run_dir", "")).strip()).expanduser()
    training_config = payload.get("training_config") if isinstance(payload.get("training_config"), dict) else {}
    if not model_path:
        return fail("model_path is required.")
    if not dataset_dir.exists():
        return fail(f"dataset_dir does not exist: {dataset_dir}")
    run_dir.mkdir(parents=True, exist_ok=True)
    checkpoints_dir = run_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    trainer_adapter_dir = run_dir / "trainer_adapter"
    trainer_adapter_dir.mkdir(parents=True, exist_ok=True)

    resume = payload.get("resume") if isinstance(payload.get("resume"), dict) else {}
    resume_checkpoint = resume.get("checkpoint") if isinstance(resume.get("checkpoint"), dict) else {}
    resume_weight_name: str | None = None
    if resume_checkpoint:
        checkpoint_dir = Path(str(resume_checkpoint.get("path", "")).strip()).expanduser()
        if checkpoint_dir.exists():
            copy_tree(checkpoint_dir, trainer_adapter_dir)
            weight_file = newest_adapter_weight(trainer_adapter_dir)
            resume_weight_name = weight_file.name if weight_file else None

    config = build_config(
        model_path=model_path,
        dataset_dir=dataset_dir,
        trainer_adapter_dir=trainer_adapter_dir,
        training_config=training_config,
        resume_weight_name=resume_weight_name,
    )
    config_path = run_dir / "config.yaml"
    config_path.write_text("\n".join(yaml_dump(config)) + "\n", encoding="utf-8")

    state = {
        "started_monotonic": time.monotonic(),
        "current_step": 0,
        "total_steps": int(training_config.get("iters", 1) or 1),
        "latest_train_loss": None,
        "latest_validation_loss": None,
        "last_checkpoint_step": 0,
        "last_checkpoint_kind": "",
        "status_message": "Launching trainer.",
        "last_heartbeat_at": 0.0,
        "last_snapshot_mtime": 0.0,
        "best_validation_loss": None,
        "best_step": 0,
        "checkpoints": [],
    }
    emit({"event": "status", "message": "Training worker started.", "progress": progress_payload(state, phase="preparing")})

    env = os.environ.copy()
    env["TRAINING_CONFIG_PATH"] = str(config_path)
    trainer_proc: subprocess.Popen[str] | None = None

    def terminate_trainer(*_args: Any) -> None:
        nonlocal trainer_proc
        if trainer_proc and trainer_proc.poll() is None:
            try:
                trainer_proc.terminate()
            except Exception:
                pass

    signal.signal(signal.SIGTERM, terminate_trainer)
    signal.signal(signal.SIGINT, terminate_trainer)

    try:
        trainer_proc = subprocess.Popen(
            trainer_command(payload, config_path),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
    except Exception as error:
        return fail(f"Failed to launch trainer subprocess: {error}")

    stdout_fd = trainer_proc.stdout.fileno() if trainer_proc.stdout else -1
    stderr_fd = trainer_proc.stderr.fileno() if trainer_proc.stderr else -1
    last_output_line = ""

    def maybe_snapshot(*, kind: str = "periodic") -> dict[str, Any] | None:
        weight_file = newest_adapter_weight(trainer_adapter_dir)
        if weight_file is None:
            return None
        mtime = weight_file.stat().st_mtime
        if kind == "periodic" and mtime <= state["last_snapshot_mtime"]:
            return None
        state["last_snapshot_mtime"] = mtime
        step = max(1, int(state["current_step"] or 1))
        checkpoint = snapshot_adapter(
            trainer_adapter_dir,
            checkpoints_dir,
            training_config,
            step=step,
            kind=kind,
            label="Latest" if kind == "latest" else "Best" if kind == "best" else f"Step {step}",
            validation_loss=state["latest_validation_loss"],
        )
        if checkpoint:
            state["last_checkpoint_step"] = int(checkpoint["step"])
            state["last_checkpoint_kind"] = str(checkpoint["kind"])
            if kind == "periodic":
                state["checkpoints"].append(checkpoint)
        return checkpoint

    while True:
        ready, _, _ = select.select([fd for fd in (stdout_fd, stderr_fd) if fd >= 0], [], [], 0.5)
        for fd in ready:
            stream = trainer_proc.stdout if fd == stdout_fd else trainer_proc.stderr
            if not stream:
                continue
            line = stream.readline()
            if not line:
                continue
            last_output_line = line.strip()
            parse_line(last_output_line, state)
            if state["latest_validation_loss"] is not None:
                best_loss = state["best_validation_loss"]
                current_loss = float(state["latest_validation_loss"])
                if best_loss is None or current_loss < best_loss:
                    state["best_validation_loss"] = current_loss
                    state["best_step"] = int(state["current_step"] or state["best_step"] or 1)
            if SAVE_PATTERN.search(last_output_line):
                maybe_snapshot(kind="periodic")
            emit({"event": "progress", "progress": progress_payload(state, phase="training")})
        now = time.monotonic()
        if now - state["last_heartbeat_at"] >= 5:
            state["last_heartbeat_at"] = now
            maybe_snapshot(kind="periodic")
            emit({"event": "status", "message": state["status_message"], "progress": progress_payload(state, phase="training")})
        if trainer_proc.poll() is not None:
            break

    maybe_snapshot(kind="periodic")
    if trainer_proc.returncode != 0:
        stderr_text = ""
        try:
            if trainer_proc.stderr:
                stderr_text = trainer_proc.stderr.read().strip()
        except Exception:
            stderr_text = ""
        return fail(stderr_text or last_output_line or "Trainer subprocess failed.")

    latest_checkpoint = maybe_snapshot(kind="latest")
    best_checkpoint: dict[str, Any] | None = None
    if state["best_step"]:
        step_dir = checkpoints_dir / f"step_{int(state['best_step']):05d}"
        if step_dir.exists():
            copy_tree(step_dir, checkpoints_dir / "best")
            best_checkpoint = {
                "id": "ckpt_best",
                "kind": "best",
                "label": "Best",
                "step": int(state["best_step"]),
                "path": str(checkpoints_dir / "best"),
                "validation_loss": state["best_validation_loss"],
                "created_at": now_iso(),
                "promoted": False,
            }
            emit({"event": "checkpoint", "checkpoint": best_checkpoint, "message": "Best checkpoint selected."})
    if best_checkpoint is None and latest_checkpoint is not None:
        copy_tree(Path(str(latest_checkpoint["path"])), checkpoints_dir / "best")
        best_checkpoint = {
            **latest_checkpoint,
            "id": "ckpt_best",
            "kind": "best",
            "label": "Best",
            "path": str(checkpoints_dir / "best"),
        }
        emit({"event": "checkpoint", "checkpoint": best_checkpoint, "message": "Best checkpoint aliased to latest."})
    prune_periodic_checkpoints(checkpoints_dir, keep_last=5)

    periodic = sorted(
        [
            checkpoint
            for checkpoint in state["checkpoints"]
            if str(checkpoint.get("path", "")).startswith(str(checkpoints_dir / "step_"))
            and Path(str(checkpoint.get("path", ""))).exists()
        ],
        key=lambda item: int(item["step"]),
    )
    checkpoints: list[dict[str, Any]] = []
    checkpoints.extend(periodic)
    if best_checkpoint:
        checkpoints.append(best_checkpoint)
    if latest_checkpoint:
        checkpoints.append(latest_checkpoint)
    progress = progress_payload(state, phase="completed")
    progress["percent"] = 100.0
    progress["phase"] = "completed"
    progress["status_message"] = "Training completed."
    summary = {
        "best_validation_loss": state["best_validation_loss"],
        "latest_validation_loss": state["latest_validation_loss"],
        "checkpoint_count": len(checkpoints),
        "periodic_checkpoint_count": len(periodic),
    }
    emit(
        {
            "event": "completed",
            "result": {
                "progress": progress,
                "summary": summary,
                "checkpoints": checkpoints,
                "best_checkpoint": best_checkpoint,
                "latest_checkpoint": latest_checkpoint,
            },
            "created_at": now_iso(),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

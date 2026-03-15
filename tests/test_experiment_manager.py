import atexit
import json
import os
import shutil
import tempfile
import types
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch


IMPORT_DATA_DIR = tempfile.mkdtemp(prefix="assist-test-import-broker-")
atexit.register(shutil.rmtree, IMPORT_DATA_DIR, ignore_errors=True)
os.environ["BROKER_DATA_DIR"] = IMPORT_DATA_DIR

from broker import local_broker


class ImmediateThread:
    def __init__(self, *args, **kwargs) -> None:
        self._target = kwargs.get("target")
        self._args = kwargs.get("args", ())
        self._kwargs = kwargs.get("kwargs", {})

    def start(self) -> None:
        if self._target:
            self._target(*self._args, **self._kwargs)

    def join(self, timeout: float | None = None) -> None:
        return None


class ExperimentManagerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="assist-test-experiments-")
        self.data_dir = Path(self.temp_dir.name)
        self.model_path = self.data_dir / "model.bin"
        self.model_path.write_text("stub", encoding="utf-8")
        self.mlx_worker_path = self.data_dir / "mlx_worker.py"
        self.mlx_worker_path.write_text("# stub\n", encoding="utf-8")
        self.experiment_worker_path = self.data_dir / "experiment_worker.py"
        self.experiment_worker_path.write_text("# stub\n", encoding="utf-8")
        codex_home = self.data_dir / "codex_home"
        codex_home.mkdir(parents=True, exist_ok=True)
        self.config = replace(
            local_broker.CONFIG,
            data_dir=self.data_dir,
            mlx_model_path=str(self.model_path),
            mlx_worker_path=self.mlx_worker_path,
            experiment_worker_path=self.experiment_worker_path,
            codex_home=codex_home,
            codex_session_index_path=self.data_dir / "codex_sessions.json",
        )
        self.manager = local_broker.ExperimentManager(self.config)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _run_job_and_get_artifact(
        self,
        payload: dict[str, object],
        *,
        worker_result: dict[str, object],
    ) -> tuple[dict[str, object], dict[str, object], list[dict[str, object]]]:
        worker_payloads: list[dict[str, object]] = []

        def fake_run_subprocess_with_cancel(*args, **kwargs):
            worker_payloads.append(json.loads(str(kwargs.get("input_text", "{}"))))
            return types.SimpleNamespace(
                returncode=0,
                stdout=json.dumps({"ok": True, "data": worker_result}),
                stderr="",
            )

        with (
            patch.object(local_broker.threading, "Thread", ImmediateThread),
            patch.object(local_broker, "run_subprocess_with_cancel", side_effect=fake_run_subprocess_with_cancel),
            patch.object(local_broker.MLX_RUNTIME, "effective_max_context_chars", return_value=4096),
        ):
            response = self.manager.start_job(payload)

        job_id = str(response["job"]["job_id"])
        job = self.manager.get_job(job_id)
        experiment_id = str((job.get("result") or {}).get("experiment_id", ""))
        artifact = self.manager.get_experiment(experiment_id)["experiment"]
        return job, artifact, worker_payloads

    def test_prompt_eval_ignores_adapter_path_and_adapter_id(self) -> None:
        worker_result = {
            "kind": "prompt_eval",
            "prompt_count": 1,
            "items": [
                {
                    "id": "prompt_01",
                    "prompt": "Say hello",
                    "reference": "",
                    "output": "hello",
                    "token_count": 2,
                    "latency_ms": 15,
                    "metrics": {"exact_match": None, "contains_reference": None},
                }
            ],
            "summary": {
                "prompt_count": 1,
                "average_latency_ms": 15,
                "exact_match_rate": None,
                "contains_reference_rate": None,
            },
        }

        with patch.object(local_broker.MLX_RUNTIME, "list_adapters", side_effect=AssertionError("unexpected adapter lookup")):
            job, artifact, worker_payloads = self._run_job_and_get_artifact(
                {
                    "kind": "prompt_eval",
                    "model_path": str(self.model_path),
                    "prompt_set": ["Say hello"],
                    "adapter_path": "/path/that/does/not/exist",
                    "adapter_id": "missing-adapter",
                },
                worker_result=worker_result,
            )

        self.assertEqual("", job["input_summary"]["adapter_path"])
        self.assertEqual("", worker_payloads[0]["adapter_path"])
        self.assertEqual("", artifact["adapter_path"])
        self.assertEqual("prompt_eval", artifact["kind"])

    def test_adapter_eval_requires_adapter_metadata(self) -> None:
        with self.assertRaisesRegex(ValueError, "adapter_path or adapter_id is required for adapter_eval."):
            self.manager.start_job(
                {
                    "kind": "adapter_eval",
                    "model_path": str(self.model_path),
                    "prompt_set": ["Say hello"],
                }
            )

    def test_adapter_eval_still_persists_resolved_adapter_path(self) -> None:
        adapter_path = self.data_dir / "adapter.safetensors"
        adapter_path.write_text("stub", encoding="utf-8")
        worker_result = {
            "kind": "adapter_eval",
            "prompt_count": 1,
            "items": [
                {
                    "id": "prompt_01",
                    "prompt": "Say hello",
                    "reference": "",
                    "base": {
                        "output": "hello",
                        "token_count": 2,
                        "latency_ms": 10,
                        "metrics": {"exact_match": None, "contains_reference": None},
                    },
                    "adapter": {
                        "output": "hello from adapter",
                        "token_count": 4,
                        "latency_ms": 12,
                        "metrics": {"exact_match": None, "contains_reference": None},
                    },
                    "changed_output": True,
                }
            ],
            "summary": {
                "prompt_count": 1,
                "base_average_latency_ms": 10,
                "adapter_average_latency_ms": 12,
                "base_exact_match_rate": None,
                "adapter_exact_match_rate": None,
                "base_contains_reference_rate": None,
                "adapter_contains_reference_rate": None,
                "improved_contains_reference_count": 0,
                "comparable_reference_items": 0,
            },
        }

        job, artifact, worker_payloads = self._run_job_and_get_artifact(
            {
                "kind": "adapter_eval",
                "model_path": str(self.model_path),
                "prompt_set": ["Say hello"],
                "adapter_path": str(adapter_path),
            },
            worker_result=worker_result,
        )

        self.assertEqual(str(adapter_path), job["input_summary"]["adapter_path"])
        self.assertEqual(str(adapter_path), worker_payloads[0]["adapter_path"])
        self.assertEqual(str(adapter_path), artifact["adapter_path"])
        self.assertEqual("adapter_eval", artifact["kind"])


if __name__ == "__main__":
    unittest.main()

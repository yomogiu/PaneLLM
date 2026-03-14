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


IMPORT_DATA_DIR = tempfile.mkdtemp(prefix="assist-test-import-training-broker-")
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


class TrainingManagerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="assist-test-training-")
        self.data_dir = Path(self.temp_dir.name)
        self.model_path = self.data_dir / "model.bin"
        self.model_path.write_text("stub", encoding="utf-8")
        self.training_worker_path = self.data_dir / "training_worker.py"
        self.training_worker_path.write_text("# stub\n", encoding="utf-8")
        codex_home = self.data_dir / "codex_home"
        codex_home.mkdir(parents=True, exist_ok=True)
        self.config = replace(
            local_broker.CONFIG,
            data_dir=self.data_dir,
            mlx_model_path=str(self.model_path),
            training_worker_path=self.training_worker_path,
            codex_home=codex_home,
            codex_session_index_path=self.data_dir / "codex_sessions.json",
        )
        self.manager = local_broker.TrainingManager(self.config)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write_dataset(self, rows: list[dict[str, object]]) -> Path:
        dataset_path = self.data_dir / "dataset.jsonl"
        dataset_path.write_text(
            "\n".join(json.dumps(row, ensure_ascii=True) for row in rows) + "\n",
            encoding="utf-8",
        )
        return dataset_path

    def test_import_dataset_generates_validation_split(self) -> None:
        source = self._write_dataset(
            [
                {"prompt": "one", "completion": "1"},
                {"prompt": "two", "completion": "2"},
                {"prompt": "three", "completion": "3"},
                {"prompt": "four", "completion": "4"},
                {"prompt": "five", "completion": "5"},
            ]
        )
        result = self.manager.import_dataset({"path": str(source)})
        dataset = result["dataset"]

        self.assertEqual("generated_validation", dataset["split_mode"])
        self.assertEqual(4, dataset["record_counts"]["train"])
        self.assertEqual(1, dataset["record_counts"]["valid"])
        self.assertTrue(self.manager._datasets.split_path(dataset["dataset_id"], "train").exists())
        self.assertTrue(self.manager._datasets.split_path(dataset["dataset_id"], "valid").exists())

    def test_start_job_streams_progress_and_promotes_checkpoints(self) -> None:
        source = self._write_dataset(
            [
                {"prompt": "alpha", "completion": "A"},
                {"prompt": "beta", "completion": "B"},
                {"prompt": "gamma", "completion": "C"},
            ]
        )
        dataset = self.manager.import_dataset({"path": str(source)})["dataset"]
        best_dir = self.data_dir / "best_ckpt"
        latest_dir = self.data_dir / "latest_ckpt"
        best_dir.mkdir(parents=True, exist_ok=True)
        latest_dir.mkdir(parents=True, exist_ok=True)

        def fake_stream(*args, **kwargs):
            on_event = kwargs["on_event"]
            on_event(
                {
                    "event": "progress",
                    "progress": {
                        "phase": "training",
                        "percent": 50.0,
                        "current_step": 300,
                        "total_steps": 600,
                        "latest_train_loss": 1.25,
                        "latest_validation_loss": 1.1,
                        "elapsed_sec": 12,
                        "eta_sec": 12,
                        "last_checkpoint_step": 300,
                        "last_checkpoint_kind": "periodic",
                        "status_message": "midway",
                    },
                }
            )
            return {
                "progress": {
                    "phase": "completed",
                    "percent": 100.0,
                    "current_step": 600,
                    "total_steps": 600,
                    "latest_train_loss": 0.8,
                    "latest_validation_loss": 0.7,
                    "elapsed_sec": 20,
                    "eta_sec": 0,
                    "last_checkpoint_step": 600,
                    "last_checkpoint_kind": "latest",
                    "status_message": "done",
                },
                "summary": {
                    "best_validation_loss": 0.7,
                    "latest_validation_loss": 0.7,
                    "checkpoint_count": 2,
                },
                "checkpoints": [
                    {
                        "id": "ckpt_best",
                        "kind": "best",
                        "label": "Best",
                        "step": 600,
                        "path": str(best_dir),
                        "validation_loss": 0.7,
                    },
                    {
                        "id": "ckpt_latest",
                        "kind": "latest",
                        "label": "Latest",
                        "step": 600,
                        "path": str(latest_dir),
                        "validation_loss": 0.7,
                    },
                ],
                "best_checkpoint": {
                    "id": "ckpt_best",
                    "kind": "best",
                    "label": "Best",
                    "step": 600,
                    "path": str(best_dir),
                    "validation_loss": 0.7,
                },
                "latest_checkpoint": {
                    "id": "ckpt_latest",
                    "kind": "latest",
                    "label": "Latest",
                    "step": 600,
                    "path": str(latest_dir),
                    "validation_loss": 0.7,
                },
            }

        with (
            patch.object(local_broker.threading, "Thread", ImmediateThread),
            patch.object(local_broker, "stream_training_worker_events", side_effect=fake_stream),
            patch.object(local_broker.MLX_RUNTIME, "status", return_value={"status": "stopped"}),
            patch.object(local_broker.MLX_RUNTIME, "register_adapter", return_value={"adapter": {}, "adapters": []}) as register_adapter,
        ):
            response = self.manager.start_job({"dataset_id": dataset["dataset_id"], "model_path": str(self.model_path)})

        job = self.manager.get_job(str(response["job"]["job_id"]))
        run = self.manager.get_run(str(job["result"]["run_id"]))["run"]

        self.assertEqual("completed", job["status"])
        self.assertEqual("completed", run["status"])
        self.assertEqual(100.0, run["progress"]["percent"])
        self.assertEqual(2, register_adapter.call_count)

    def test_resume_job_uses_prior_run_metadata(self) -> None:
        source = self._write_dataset(
            [
                {"prompt": "alpha", "completion": "A"},
                {"prompt": "beta", "completion": "B"},
            ]
        )
        dataset = self.manager.import_dataset({"path": str(source)})["dataset"]
        checkpoint_dir = self.data_dir / "resume_ckpt"
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.manager._runs.create(
            {
                "run_id": "trn_base123456",
                "job_id": "training_job_base",
                "name": "base run",
                "status": "completed",
                "phase": "completed",
                "dataset_id": dataset["dataset_id"],
                "dataset": self.manager._dataset_summary(dataset),
                "model_path": str(self.model_path),
                "training_config": dict(local_broker.TRAINING_BALANCED_PROFILE),
                "created_at": local_broker.now_iso(),
                "updated_at": local_broker.now_iso(),
                "completed_at": local_broker.now_iso(),
                "progress": {},
                "checkpoints": [
                    {
                        "id": "ckpt_latest",
                        "kind": "latest",
                        "label": "Latest",
                        "step": 600,
                        "path": str(checkpoint_dir),
                        "validation_loss": 0.9,
                    }
                ],
                "best_checkpoint": None,
                "latest_checkpoint": {
                    "id": "ckpt_latest",
                    "kind": "latest",
                    "label": "Latest",
                    "step": 600,
                    "path": str(checkpoint_dir),
                    "validation_loss": 0.9,
                },
                "summary": {},
                "error": None,
                "resume": None,
            }
        )
        captured_payloads: list[dict[str, object]] = []

        def fake_stream(*args, **kwargs):
            captured_payloads.append(dict(kwargs["input_payload"]))
            return {
                "progress": {
                    "phase": "completed",
                    "percent": 100.0,
                    "current_step": 50,
                    "total_steps": 50,
                    "latest_train_loss": 0.5,
                    "latest_validation_loss": 0.5,
                    "elapsed_sec": 5,
                    "eta_sec": 0,
                    "last_checkpoint_step": 50,
                    "last_checkpoint_kind": "latest",
                    "status_message": "done",
                },
                "summary": {},
                "checkpoints": [],
                "best_checkpoint": None,
                "latest_checkpoint": None,
            }

        with (
            patch.object(local_broker.threading, "Thread", ImmediateThread),
            patch.object(local_broker, "stream_training_worker_events", side_effect=fake_stream),
            patch.object(local_broker.MLX_RUNTIME, "status", return_value={"status": "stopped"}),
            patch.object(local_broker.MLX_RUNTIME, "register_adapter", return_value={"adapter": {}, "adapters": []}),
        ):
            response = self.manager.start_job(
                {
                    "resume_run_id": "trn_base123456",
                    "additional_iters": 50,
                }
            )

        self.assertEqual("completed", response["job"]["status"])
        self.assertEqual("trn_base123456", captured_payloads[0]["resume"]["run_id"])
        self.assertEqual(50, captured_payloads[0]["training_config"]["iters"])

    def test_promote_checkpoint_registers_adapter(self) -> None:
        checkpoint_dir = self.data_dir / "promote_ckpt"
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.manager._runs.create(
            {
                "run_id": "trn_promote123",
                "job_id": "training_job_promote",
                "name": "promote",
                "status": "completed",
                "phase": "completed",
                "dataset_id": "ds_demo",
                "dataset": {},
                "model_path": str(self.model_path),
                "training_config": dict(local_broker.TRAINING_BALANCED_PROFILE),
                "created_at": local_broker.now_iso(),
                "updated_at": local_broker.now_iso(),
                "completed_at": local_broker.now_iso(),
                "progress": {},
                "checkpoints": [
                    {
                        "id": "ckpt_best",
                        "kind": "best",
                        "label": "Best",
                        "step": 600,
                        "path": str(checkpoint_dir),
                        "validation_loss": 0.5,
                    }
                ],
                "best_checkpoint": {
                    "id": "ckpt_best",
                    "kind": "best",
                    "label": "Best",
                    "step": 600,
                    "path": str(checkpoint_dir),
                    "validation_loss": 0.5,
                },
                "latest_checkpoint": None,
                "summary": {},
                "error": None,
                "resume": None,
            }
        )
        with patch.object(
            local_broker.MLX_RUNTIME,
            "register_adapter",
            return_value={"adapter": {"id": "adp_saved"}, "adapters": []},
        ) as register_adapter:
            result = self.manager.promote_checkpoint({"run_id": "trn_promote123", "checkpoint_kind": "best"})
        self.assertTrue(result["ok"])
        self.assertEqual("adp_saved", result["adapter"]["id"])
        register_adapter.assert_called_once()


if __name__ == "__main__":
    unittest.main()

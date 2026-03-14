import json
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


WORKER_PATH = Path(__file__).resolve().parents[1] / "broker" / "training_worker.py"


class TrainingWorkerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="assist-test-training-worker-")
        self.root = Path(self.temp_dir.name)
        self.dataset_dir = self.root / "dataset"
        self.dataset_dir.mkdir(parents=True, exist_ok=True)
        (self.dataset_dir / "train.jsonl").write_text('{"prompt":"hi","completion":"hello"}\n', encoding="utf-8")
        (self.dataset_dir / "valid.jsonl").write_text('{"prompt":"bye","completion":"goodbye"}\n', encoding="utf-8")
        self.run_dir = self.root / "run"
        self.run_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write_trainer(self, source: str, name: str) -> Path:
        path = self.root / name
        path.write_text(textwrap.dedent(source), encoding="utf-8")
        return path

    def _run_worker(self, trainer_path: Path) -> subprocess.CompletedProcess[str]:
        payload = {
            "job_id": "training_job_test",
            "run_id": "trn_testworker",
            "run_dir": str(self.run_dir),
            "dataset_dir": str(self.dataset_dir),
            "model_path": str(self.root / "model"),
            "training_config": {
                "rank": 8,
                "scale": 20,
                "dropout": 0,
                "num_layers": 8,
                "learning_rate": 0.00001,
                "iters": 6,
                "batch_size": 1,
                "grad_accumulation_steps": 4,
                "steps_per_report": 1,
                "steps_per_eval": 2,
                "save_every": 2,
                "val_batches": 2,
                "max_seq_length": 512,
                "grad_checkpoint": True,
                "seed": 0,
            },
            "trainer_command": [sys.executable, str(trainer_path)],
        }
        return subprocess.run(
            ["python3", str(WORKER_PATH)],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=False,
        )

    def test_worker_emits_progress_and_checkpoints(self) -> None:
        trainer = self._write_trainer(
            """
            import re
            import sys
            import time
            from pathlib import Path

            config_path = Path(sys.argv[-1])
            text = config_path.read_text(encoding="utf-8")
            adapter_path = re.search(r'^adapter_path:\\s+"?([^"\\n]+)"?$', text, re.M).group(1)
            adapter_dir = Path(adapter_path)
            adapter_dir.mkdir(parents=True, exist_ok=True)
            for step, train_loss, val_loss in [(2, 1.2, 1.0), (4, 0.9, 0.8), (6, 0.7, 0.6)]:
                (adapter_dir / "adapters.safetensors").write_text(f"step={step}", encoding="utf-8")
                print(f"Step {step}/6 train loss {train_loss}", flush=True)
                print(f"Validation loss {val_loss} at step {step}", flush=True)
                print("Saved adapter checkpoint", flush=True)
                time.sleep(0.05)
            """,
            "trainer_ok.py",
        )
        result = self._run_worker(trainer)
        self.assertEqual(0, result.returncode, msg=result.stderr)
        events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
        event_names = [event.get("event") for event in events]
        self.assertIn("completed", event_names)
        self.assertIn("checkpoint", event_names)
        completed = next(event for event in events if event.get("event") == "completed")
        self.assertEqual("completed", completed["result"]["progress"]["phase"])
        self.assertTrue(completed["result"]["latest_checkpoint"]["path"])

    def test_worker_reports_failure(self) -> None:
        trainer = self._write_trainer(
            """
            import sys
            print("trainer exploded", file=sys.stderr, flush=True)
            raise SystemExit(3)
            """,
            "trainer_fail.py",
        )
        result = self._run_worker(trainer)
        self.assertNotEqual(0, result.returncode)
        events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
        self.assertTrue(any(event.get("event") == "error" for event in events))


if __name__ == "__main__":
    unittest.main()

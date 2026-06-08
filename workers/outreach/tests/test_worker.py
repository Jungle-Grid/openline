import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WORKER = ROOT / "workers" / "outreach" / "outreach_worker.py"
sys.path.insert(0, str(ROOT))
from workers.outreach import outreach_worker as worker_module


class WorkerSmokeTest(unittest.TestCase):
    def test_template_run_writes_valid_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            env = {
                **os.environ,
                "FIT_SCORE_THRESHOLD": "60",
                "MAX_DRAFTS_PER_DOMAIN": "2",
            }
            subprocess.run(
                [
                    sys.executable,
                    str(WORKER),
                    "--job",
                    "full-run-template",
                    "--target",
                    "2",
                    "--output",
                    directory,
                    "--input",
                    str(ROOT / "examples" / "sample-worker-input.json"),
                ],
                check=True,
                env=env,
            )
            artifacts = {path.name for path in Path(directory).glob("*.json")}
            self.assertEqual(
                artifacts,
                {
                    "prospects.json",
                    "research_notes.json",
                    "scored_prospects.json",
                    "email_drafts.json",
                    "run_summary.json",
                    "validation_report.json",
                },
            )
            drafts = json.loads((Path(directory) / "email_drafts.json").read_text())
            self.assertGreaterEqual(len(drafts), 1)
            for draft in drafts:
                self.assertEqual(draft["links"], ["https://junglegrid.dev"])
                self.assertGreaterEqual(draft["word_count"], 60)
                self.assertLessEqual(draft["word_count"], 80)

    def test_qwen_invalid_output_falls_back_to_template_validation(self):
        prospect = {
            "prospect_id": "p1",
            "name": "Avery Maintainer",
            "email": "hello@agent-runtime.dev",
            "email_source_url": "https://agent-runtime.dev/contact",
            "project": "sample/agent-runtime",
            "category": "agent_compute",
            "fit_score": 92,
        }
        note = {
            "prospect_id": "p1",
            "summary": "Agent Runtime documents durable worker jobs.",
            "personalization_detail": "its durable worker queue preserves logs and output artifacts",
            "junglegrid_relevance": "The workload needs durable compute execution.",
            "evidence_urls": [
                "https://agent-runtime.dev/contact",
                "https://github.com/sample/agent-runtime#readme",
            ],
            "evidence_strength": 0.9,
        }
        env = {
            **os.environ,
            "FIT_SCORE_THRESHOLD": "60",
            "LLM_FALLBACK_MODE": "template",
            "USE_LOCAL_LLM": "true",
        }
        with patch.dict(os.environ, env, clear=False):
            with patch.object(worker_module, "ensure_ollama", return_value=True):
                with patch.object(
                    worker_module,
                    "qwen_draft",
                    return_value=("Short note", "Too short https://invalid.test", ["invalid claim"]),
                ):
                    drafts, failures, fallback_used = worker_module.write_drafts([prospect], [note], True)
        self.assertTrue(fallback_used)
        self.assertEqual(len(failures), 0)
        self.assertEqual(len(drafts), 1)
        self.assertEqual(drafts[0]["model_mode"], "fallback")
        self.assertEqual(drafts[0]["links"], ["https://junglegrid.dev"])
        self.assertGreaterEqual(drafts[0]["word_count"], 60)
        self.assertLessEqual(drafts[0]["word_count"], 80)

    def test_qwen_mode_falls_back_to_templates_when_runtime_is_unavailable(self):
        with tempfile.TemporaryDirectory() as directory:
            env = {
                **os.environ,
                "FIT_SCORE_THRESHOLD": "60",
                "OLLAMA_HOST": "http://127.0.0.1:9",
                "LLM_FALLBACK_MODE": "template",
            }
            subprocess.run(
                [
                    sys.executable,
                    str(WORKER),
                    "--job",
                    "full-run-qwen",
                    "--target",
                    "1",
                    "--output",
                    directory,
                    "--input",
                    str(ROOT / "examples" / "sample-worker-input.json"),
                ],
                check=True,
                env=env,
            )
            summary = json.loads((Path(directory) / "run_summary.json").read_text())
            drafts = json.loads((Path(directory) / "email_drafts.json").read_text())
            self.assertTrue(summary["fallback_used"])
            self.assertEqual(drafts[0]["model_mode"], "fallback")

    def test_discover_skips_env_excluded_contacts(self):
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory) / "input.json"
            input_path.write_text((ROOT / "examples" / "sample-worker-input.json").read_text())
            env = {
                **os.environ,
                "OUTREACH_EXCLUDED_EMAILS": json.dumps(["hello@agent-runtime.dev"]),
            }
            with patch.dict(os.environ, env, clear=False):
                prospects = worker_module.discover(2, input_path, None)
            emails = {row["email"] for row in prospects}
            self.assertNotIn("hello@agent-runtime.dev", emails)


if __name__ == "__main__":
    unittest.main()

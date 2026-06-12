import json
import os
import subprocess
import sys
import tempfile
import unittest
import http.client
import urllib.error
from datetime import datetime
from unittest.mock import patch
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WORKER = ROOT / "workers" / "outreach" / "outreach_worker.py"
sys.path.insert(0, str(ROOT))
from workers.outreach import outreach_worker as worker_module
from workers.outreach.source_adapters import (
    DiscoveryContext,
    DiscoveryQuery,
    RawSourceDocument,
    SourceCandidate,
    SourceHealth,
    build_default_registry,
)


class WorkerSmokeTest(unittest.TestCase):
    def test_public_contact_points_extract_supported_non_email_channels(self):
        html = """
        <a href="/contact">Contact sales</a>
        <a href="tel:+15551234567">Call us</a>
        <a href="https://wa.me/15551234567">WhatsApp</a>
        <a href="https://github.com/acme/runtime/discussions">Discussions</a>
        <a href="https://www.linkedin.com/company/acme">LinkedIn</a>
        <a href="https://discord.gg/acme">Discord</a>
        <a href="https://cal.com/acme/demo">Book a demo</a>
        <form action="/partnerships/apply"></form>
        """
        points = worker_module.public_contact_points(html, "https://acme.dev")
        point_types = {point["type"] for point in points}
        self.assertTrue(
            {
                "official_contact_form",
                "business_phone",
                "whatsapp_business",
                "github_discussions",
                "linkedin_company",
                "discord",
                "booking_link",
                "partnership_form",
            }.issubset(point_types)
        )

    def test_scheduled_conversation_turn_does_not_require_inbound_body(self):
        payload = {
            "trigger": "scheduled_follow_up",
            "inbound_body": "",
            "history": [{"direction": "outbound", "body": "Prior note"}],
        }
        analysis = {
            "response": json.dumps(
                {
                    "classification": "other",
                    "summary": "The scheduled follow-up remains appropriate.",
                    "open_questions": [],
                    "commitments": [],
                    "objections": [],
                    "follow_up_at": None,
                    "opportunity_state": "engaged",
                    "next_action": "respond",
                    "response_subject": "Following up",
                    "response_body": "Following up with the requested implementation note.",
                    "escalation_required": False,
                }
            )
        }
        with patch.object(worker_module, "request_json", return_value=analysis):
            result = worker_module.qwen_conversation_turn(payload, "qwen-test")
        self.assertEqual(result["next_action"], "respond")
        self.assertEqual(result["opportunity_state"], "engaged")

    def test_conversation_turn_uses_structured_analysis_and_validation(self):
        analysis = {
            "response": json.dumps(
                {
                    "classification": "question",
                    "summary": "The prospect asks about a pilot.",
                    "open_questions": ["What does the pilot include?"],
                    "commitments": [],
                    "objections": [],
                    "follow_up_at": None,
                    "opportunity_state": "evaluating",
                    "next_action": "respond",
                    "response_subject": "Re: pilot",
                    "response_body": "Thanks. I can share a bounded pilot outline.",
                    "escalation_required": False,
                }
            )
        }
        validation = {
            "response": json.dumps({"status": "send_ready", "reasons": []})
        }
        with patch.object(
            worker_module, "request_json", side_effect=[analysis, validation]
        ):
            result = worker_module.qwen_conversation_turn(
                {"inbound_body": "What does a pilot include?", "history": []},
                "qwen-test",
            )
            status, reasons = worker_module.qwen_validate_conversation_response(
                {"inbound_body": "What does a pilot include?", "history": []},
                result,
                "qwen-test",
            )
        self.assertEqual(result["classification"], "question")
        self.assertEqual(status, "send_ready")
        self.assertEqual(reasons, [])

    def test_non_email_contact_can_qualify_without_becoming_an_email_draft(self):
        campaign = json.loads(
            (ROOT / "config" / "campaigns" / "jungle-grid.json").read_text()
        )
        raw = {
            "prospect_id": "contact-form-only",
            "name": "Avery Builder",
            "project": "avery/agent-runtime",
            "project_url": "https://github.com/avery/agent-runtime",
            "project_description": "AI agent runtime",
            "category": "agent_compute",
            "contact_points": [
                {
                    "type": "official_contact_form",
                    "value": "https://agent-runtime.dev/contact",
                    "source_url": "https://agent-runtime.dev/contact",
                    "publicly_listed": True,
                    "authorized": False,
                    "confidence": 0.9,
                }
            ],
            "research_text": (
                "A maintainer-built AI agent runtime executes long-running LLM inference "
                "jobs with background workers, retries, logs, artifacts, GPU capacity, "
                "and deployment monitoring for production workloads."
            ),
            "updated_at": worker_module.utc_now(),
            "owner_login": "avery",
            "owner_type": "User",
        }
        with patch.object(worker_module, "CAMPAIGN", campaign):
            prospect = worker_module.normalize_prospect(raw)
            self.assertIsNotNone(prospect)
            diagnostics = worker_module.qualification_diagnostics(prospect)
        self.assertFalse(diagnostics["excluded"])
        self.assertEqual(prospect["email"], "")
        self.assertEqual(prospect["contact_points"][0]["type"], "official_contact_form")

    def test_source_registry_loads_yaml_and_restricted_env_overrides(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "sources.yaml"
            config_path.write_text(
                """
sources:
  github:
    enabled: false
  hackernews:
    enabled: true
    retry_count: 4
restricted_sources:
  discord:
    enabled: false
""".strip()
            )
            with patch.dict(
                os.environ,
                {
                    "OUTREACH_SOURCES_CONFIG": str(config_path),
                    "ENABLE_DISCORD_SOURCE": "true",
                },
                clear=False,
            ):
                config = worker_module.source_registry_config()
                registry = build_default_registry(config)

            self.assertFalse(config["sources"]["github"]["enabled"])
            self.assertTrue(config["restricted_sources"]["discord"]["enabled"])
            self.assertEqual(registry.get("hackernews").capabilities.retry_count, 4)
            self.assertEqual(registry.get("github").health_check().status, "disabled")
            self.assertEqual(registry.get("discord").health_check().status, "disabled")

    def test_qwen_semantic_analysis_drives_qualification_and_explanations(self):
        prospects = [
            {
                "prospect_id": "p1",
                "project": "acme/runtime",
                "category": "agent_compute",
                "project_description": "Agent runtime",
                "diagnostics": {"has_campaign_workload_signal": True},
            },
            {
                "prospect_id": "p2",
                "project": "acme/utility",
                "category": "other",
                "project_description": "Generic utility",
                "diagnostics": {"has_campaign_workload_signal": False},
            },
        ]
        notes = [
            {
                "prospect_id": "p1",
                "summary": "Runtime executes long-running jobs.",
                "personalization_detail": "Long-running jobs retain logs.",
                "junglegrid_relevance": "Execution fit.",
                "evidence_urls": ["https://example.dev/runtime"],
            },
            {
                "prospect_id": "p2",
                "summary": "Utility formats strings.",
                "personalization_detail": "Formats strings.",
                "junglegrid_relevance": "No demonstrated fit.",
                "evidence_urls": ["https://example.dev/utility"],
            },
        ]
        response = {
            "response": json.dumps(
                {
                    "items": [
                        {
                            "prospect_id": "p1",
                            "qualified": True,
                            "qualification_reason": "Direct durable workload evidence.",
                            "research_analysis": "The runtime executes long-running jobs and retains logs.",
                            "score_explanation": "Primary evidence supports workload and integration fit.",
                            "suggested_angle": "Execution substrate beneath the existing runtime.",
                        },
                        {
                            "prospect_id": "p2",
                            "qualified": False,
                            "qualification_reason": "No campaign workload evidence.",
                            "research_analysis": "The evidence describes only string formatting.",
                            "score_explanation": "No supported campaign dimensions.",
                            "suggested_angle": "Do not contact.",
                        },
                    ]
                }
            )
        }
        with patch.object(worker_module, "request_json", return_value=response):
            analysis = worker_module.qwen_semantic_analysis(prospects, notes, "qwen-test")
        accepted, accepted_notes, excluded = worker_module.apply_semantic_analysis(
            prospects, notes, analysis
        )
        self.assertEqual([item["prospect_id"] for item in accepted], ["p1"])
        self.assertEqual(excluded[0]["exclusion_rule_triggered"], "semantic_qualification_rejected")
        self.assertIn("execution substrate", accepted_notes[0]["semantic_suggested_angle"].lower())
        self.assertTrue(accepted_notes[0]["semantic_score_explanation"])

    def test_qwen_semantic_analysis_accepts_wrapped_items_payload(self):
        prospects = [
            {
                "prospect_id": "p1",
                "project": "acme/runtime",
                "category": "agent_compute",
                "project_description": "Agent runtime",
                "diagnostics": {"has_campaign_workload_signal": True},
            }
        ]
        notes = [
            {
                "prospect_id": "p1",
                "summary": "Runtime executes long-running jobs.",
                "personalization_detail": "Long-running jobs retain logs.",
                "junglegrid_relevance": "Execution fit.",
                "evidence_urls": ["https://example.dev/runtime"],
            }
        ]
        response = {
            "response": json.dumps(
                {
                    "analysis": {
                        "items": [
                            {
                                "prospect_id": "p1",
                                "qualified": True,
                                "qualification_reason": "Direct durable workload evidence.",
                                "research_analysis": "The runtime executes long-running jobs and retains logs.",
                                "score_explanation": "Primary evidence supports workload and integration fit.",
                                "suggested_angle": "Execution substrate beneath the existing runtime.",
                            }
                        ]
                    }
                }
            )
        }
        with patch.object(worker_module, "request_json", return_value=response):
            analysis = worker_module.qwen_semantic_analysis(prospects, notes, "qwen-test")
        self.assertIn("p1", analysis)
        self.assertTrue(analysis["p1"]["qualified"])

    def test_qwen_json_parser_accepts_fenced_json(self):
        parsed = worker_module.parse_qwen_json_response(
            {"response": '```json\n{"status":"send_ready","reasons":[]}\n```'}
        )
        self.assertEqual(parsed["status"], "send_ready")

    def test_qwen_semantic_validation_rejects_unsupported_draft(self):
        drafts = [
            {
                "prospect_id": "p1",
                "subject": "Relevant",
                "body": "Supported body",
                "personalization_claims": ["supported claim"],
            },
            {
                "prospect_id": "p2",
                "subject": "Unsupported",
                "body": "Unsupported pain claim",
                "personalization_claims": ["unsupported claim"],
            },
        ]
        notes = [
            {"prospect_id": "p1", "evidence": [{"claim": "supported claim"}]},
            {"prospect_id": "p2", "evidence": [{"claim": "different evidence"}]},
        ]
        response = {
            "response": json.dumps(
                {
                    "items": [
                        {"prospect_id": "p1", "status": "send_ready", "reasons": []},
                        {
                            "prospect_id": "p2",
                            "status": "regeneration_required",
                            "reasons": ["Pain claim is unsupported by evidence."],
                        },
                    ]
                }
            )
        }
        with patch.object(worker_module, "request_json", return_value=response):
            validation = worker_module.qwen_semantic_validate_drafts(
                drafts, notes, "qwen-test"
            )
        accepted, failures = worker_module.apply_semantic_draft_validation(
            drafts, [], validation
        )
        self.assertEqual([item["prospect_id"] for item in accepted], ["p1"])
        self.assertEqual(failures[0]["validation_status"], "regeneration_required")
        self.assertIn("unsupported", failures[0]["errors"][0].lower())

    def test_generic_campaign_uses_distinct_criteria_and_offer(self):
        with tempfile.TemporaryDirectory() as directory:
            campaign = json.loads((ROOT / "config" / "campaigns" / "generic-saas.json").read_text())
            input_path = Path(directory) / "input.json"
            input_path.write_text(
                json.dumps(
                    {
                        "campaign_configuration": campaign,
                        "prospects": [
                            {
                                "prospect_id": "generic-saas",
                                "name": "Avery Builder",
                                "email": "avery@releasewatch.dev",
                                "email_source_url": "https://releasewatch.dev/contact",
                                "email_source_type": "official_website",
                                "project": "avery/releasewatch",
                                "project_url": "https://github.com/avery/releasewatch",
                                "project_description": "A developer tool for monitoring SaaS API releases.",
                                "category": "developer_tool",
                                "research_text": "ReleaseWatch operates a production web service for SaaS API teams. It connects deployment releases to logs, exceptions, and monitoring traces so maintainers can debug regressions and production incidents.",
                                "evidence_urls": ["https://github.com/avery/releasewatch#readme"],
                                "owner_login": "avery",
                                "owner_type": "User",
                                "updated_at": worker_module.utc_now(),
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            env = {
                **os.environ,
                "FIT_SCORE_THRESHOLD": "50",
                "OUTREACH_MEMORY_PATH": str(Path(directory) / "memory.json"),
            }
            subprocess.run(
                [
                    sys.executable,
                    str(WORKER),
                    "--job",
                    "full-run-template",
                    "--target",
                    "1",
                    "--output",
                    directory,
                    "--input",
                    str(input_path),
                ],
                check=True,
                env=env,
            )
            summary = json.loads((Path(directory) / "run_summary.json").read_text())
            drafts = json.loads((Path(directory) / "email_drafts.json").read_text())
            scored = json.loads((Path(directory) / "scored_prospects.json").read_text())

            self.assertEqual(summary["campaign_id"], "generic-saas-observability")
            self.assertEqual(summary["execution_backend"], "jungle_grid_mock")
            self.assertFalse(summary["production_eligible"])
            self.assertEqual(len(drafts), 1)
            self.assertIn("Trace Harbor", drafts[0]["body"])
            self.assertEqual(drafts[0]["links"], ["https://traceharbor.example"])
            self.assertNotIn("Jungle Grid", drafts[0]["body"])
            self.assertEqual(scored[0]["proof_artifacts"][0]["junglegrid_job_id"], "fixture-job")
            self.assertEqual(scored[0]["proof_artifacts"][0]["type"], "website_audit")
            for criterion, value in scored[0]["score_breakdown"].items():
                if value > 0:
                    self.assertTrue(scored[0]["score_evidence_ids"][criterion])

    def test_non_technical_campaign_runs_through_the_same_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            campaign = json.loads(
                (ROOT / "config" / "campaigns" / "local-services.json").read_text()
            )
            env = {
                **os.environ,
                "OUTREACH_CAMPAIGN_CONFIG": json.dumps(campaign),
                "FIT_SCORE_THRESHOLD": "50",
                "OUTREACH_MEMORY_PATH": str(Path(directory) / "memory.json"),
            }
            subprocess.run(
                [
                    sys.executable,
                    str(WORKER),
                    "--job",
                    "full-run-template",
                    "--target",
                    "1",
                    "--output",
                    directory,
                    "--input",
                    str(ROOT / "examples" / "sample-local-services-input.json"),
                ],
                check=True,
                env=env,
            )
            summary = json.loads((Path(directory) / "run_summary.json").read_text())
            scored = json.loads((Path(directory) / "scored_prospects.json").read_text())
            self.assertEqual(summary["campaign_id"], "local-services-booking")
            self.assertEqual(len(scored), 1)
            self.assertEqual(scored[0]["category"], "other")
            self.assertEqual(scored[0]["proof_artifacts"][0]["type"], "website_audit")

    def test_template_run_writes_valid_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            env = {
                **os.environ,
                "FIT_SCORE_THRESHOLD": "60",
                "MAX_DRAFTS_PER_DOMAIN": "2",
                "OUTREACH_MEMORY_PATH": str(Path(directory) / "state" / "memory.json"),
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
                self.assertGreaterEqual(draft["word_count"], 70)
                self.assertLessEqual(draft["word_count"], 140)
            report = json.loads((Path(directory) / "validation_report.json").read_text())
            self.assertIn("skipped_prospects", report)
            summary = json.loads((Path(directory) / "run_summary.json").read_text())
            self.assertIn("quality_metrics", summary)
            self.assertIn("exclusion_reasons", summary)
            self.assertEqual(
                summary["quality_metrics"]["scored_criteria_with_evidence_ids_percentage"],
                100.0,
            )

    def test_qwen_invalid_output_falls_back_to_template_validation(self):
        prospect = {
            "prospect_id": "p1",
            "name": "Avery Maintainer",
            "email": "avery@agent-runtime.dev",
            "email_source_url": "https://agent-runtime.dev/contact",
            "project": "sample/agent-runtime",
            "category": "agent_compute",
            "fit_score": 92,
            "contact_quality": 9,
            "excluded": False,
            "evidence_points": [
                "long-running tool jobs go to isolated workers",
                "logs, retries, and output artifacts stay attached to the job",
                "background worker execution handles model and tool workloads",
            ],
        }
        note = {
            "prospect_id": "p1",
            "summary": "Agent Runtime documents durable worker jobs.",
            "personalization_detail": "its durable worker queue preserves logs and output artifacts",
            "junglegrid_relevance": "The workload needs durable compute execution.",
            "pain_signals": ["durable worker queue preserves logs and output artifacts"],
            "evidence_points": [
                "long-running tool jobs go to isolated workers",
                "logs, retries, and output artifacts stay attached to the job",
                "background worker execution handles model and tool workloads",
            ],
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
            "OUTREACH_MEMORY_PATH": str(ROOT / "data" / "test-memory.json"),
        }
        with patch.dict(os.environ, env, clear=False):
            with patch.object(worker_module, "ensure_ollama", return_value=True):
                with patch.object(
                    worker_module,
                    "qwen_draft",
                    return_value=("Short note", "Too short https://invalid.test", ["invalid claim"]),
                ):
                    drafts, failures, fallback_used, metrics = worker_module.write_drafts([prospect], [note], True)
        self.assertTrue(fallback_used)
        self.assertEqual(metrics["fallback_generated"], 1)
        self.assertEqual(metrics["primary_generated"], 0)
        self.assertEqual(len(failures), 0)
        self.assertEqual(len(drafts), 1)
        self.assertEqual(drafts[0]["model_mode"], "fallback")
        self.assertEqual(drafts[0]["validation_status"], "manual_review_required")
        self.assertIn("fallback generation requires manual review", drafts[0]["validation_errors"])
        self.assertEqual(drafts[0]["links"], ["https://junglegrid.dev"])
        self.assertGreaterEqual(drafts[0]["word_count"], 70)
        self.assertLessEqual(drafts[0]["word_count"], 140)

    def test_qwen_mode_falls_back_to_templates_when_runtime_is_unavailable(self):
        with tempfile.TemporaryDirectory() as directory:
            env = {
                **os.environ,
                "FIT_SCORE_THRESHOLD": "60",
                "OLLAMA_HOST": "http://127.0.0.1:9",
                "LLM_FALLBACK_MODE": "template",
                "OUTREACH_MEMORY_PATH": str(Path(directory) / "state" / "memory.json"),
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
            self.assertEqual(summary["status"], "degraded")
            self.assertEqual(summary["primary_model_generated"], 0)
            self.assertEqual(summary["fallback_generated"], len(drafts))
            self.assertEqual(drafts[0]["model_mode"], "fallback")

    def test_discover_skips_env_excluded_contacts(self):
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory) / "input.json"
            input_path.write_text((ROOT / "examples" / "sample-worker-input.json").read_text())
            env = {
                **os.environ,
                "OUTREACH_EXCLUDED_EMAILS": json.dumps(["avery@agent-runtime.dev"]),
                "OUTREACH_MEMORY_PATH": str(Path(directory) / "state" / "memory.json"),
            }
            with patch.dict(os.environ, env, clear=False):
                prospects, skipped, _signals = worker_module.discover(2, input_path, None)
            emails = {row["email"] for row in prospects}
            self.assertNotIn("avery@agent-runtime.dev", emails)
            self.assertTrue(any(row["exclusion_rule_triggered"] for row in skipped))

    def test_website_contacts_ignores_remote_disconnects(self):
        with patch.object(
            worker_module,
            "fetch_text",
            side_effect=http.client.RemoteDisconnected("Remote end closed connection without response"),
        ):
            contacts, text = worker_module.website_contacts("https://example.com")
        self.assertEqual(contacts, [])
        self.assertEqual(text, "")

    def test_bad_prospect_rules_and_readme_cleaning(self):
        microsoft = worker_module.normalize_prospect(
            {
                "prospect_id": "msft",
                "name": "ONNX Runtime",
                "email": "opensource@microsoft.com",
                "email_source_url": "https://opensource.microsoft.com",
                "project": "microsoft/onnxruntime",
                "project_url": "https://github.com/microsoft/onnxruntime",
                "project_description": "GPU inference runtime SDK",
                "research_text": '<img src="badge"> ![build](https://img.shields.io/x) <a href="#">docs</a> inference server',
                "owner_login": "microsoft",
                "updated_at": datetime.utcnow().isoformat() + "Z",
            }
        )
        aws_sdk = worker_module.normalize_prospect(
            {
                "prospect_id": "aws",
                "name": "AWS SDK",
                "email": "support@aws.amazon.com",
                "email_source_url": "https://aws.amazon.com/contact-us/",
                "project": "aws/aws-sdk-js",
                "project_url": "https://github.com/aws/aws-sdk-js",
                "project_description": "SDK for AWS",
                "research_text": "SDK examples docs",
                "owner_login": "aws",
                "updated_at": datetime.utcnow().isoformat() + "Z",
            }
        )
        proxy = worker_module.normalize_prospect(
            {
                "prospect_id": "proxy",
                "name": "Global Agent",
                "email": "maintainer@global-agent.dev",
                "email_source_url": "https://global-agent.dev/contact",
                "project": "gajus/global-agent",
                "project_url": "https://github.com/gajus/global-agent",
                "project_description": "HTTP proxy agent for Node.js",
                "research_text": "Global-agent sets a global HTTP proxy agent for Node.js requests.",
                "owner_login": "gajus",
                "updated_at": datetime.utcnow().isoformat() + "Z",
            }
        )
        generic = worker_module.normalize_prospect(
            {
                "prospect_id": "generic",
                "name": "Generic",
                "email": "opensource@tool.dev",
                "email_source_url": "https://tool.dev/contact",
                "project": "solo/tool",
                "project_url": "https://github.com/solo/tool",
                "project_description": "AI worker runtime",
                "research_text": "AI worker runtime with background jobs and artifacts.",
                "owner_login": "solo",
                "updated_at": datetime.utcnow().isoformat() + "Z",
            }
        )

        self.assertEqual(worker_module.category_for(proxy["research_text"]), "open_source_ai")
        self.assertNotIn("<img", microsoft["research_text"])
        self.assertNotIn("shields.io", microsoft["research_text"])
        self.assertEqual(worker_module.qualification_diagnostics(microsoft)["exclusion_rule_triggered"], "large_vendor_or_foundation_org")
        self.assertEqual(worker_module.qualification_diagnostics(aws_sdk)["exclusion_rule_triggered"], "large_vendor_or_foundation_org")
        self.assertLess(worker_module.qualification_diagnostics(generic)["contact_quality"], 7)

    def test_good_prospect_scores_and_buyer_fit_gate(self):
        now = datetime.utcnow().isoformat() + "Z"
        prospects = [
            worker_module.normalize_prospect(
                {
                    "prospect_id": "good-1",
                    "name": "Avery Founder",
                    "email": "avery@agentfoundry.dev",
                    "email_source_url": "https://agentfoundry.dev/contact",
                    "project": "avery/agentfoundry",
                    "project_url": "https://github.com/avery/agentfoundry",
                    "project_description": "Solo-built agent runtime for long-running jobs.",
                    "research_text": "We run long-running agent jobs through a background worker queue, keep logs and artifacts, and are working through deployment latency.",
                    "owner_login": "avery",
                    "owner_type": "User",
                    "updated_at": now,
                }
            ),
            worker_module.normalize_prospect(
                {
                    "prospect_id": "good-2",
                    "name": "Morgan Maintainer",
                    "email": "morgan@mcpkit.dev",
                    "email_source_url": "https://mcpkit.dev/about",
                    "project": "morgan/mcpkit",
                    "project_url": "https://github.com/morgan/mcpkit",
                    "project_description": "MCP server for batch inference workflows.",
                    "research_text": "Our MCP server launches batch inference tasks, stores generated artifacts, and retries failed worker jobs.",
                    "owner_login": "morgan",
                    "owner_type": "User",
                    "updated_at": now,
                }
            ),
        ]

        for prospect in prospects:
            prospect["diagnostics"] = worker_module.qualification_diagnostics(prospect)
        notes = worker_module.research(prospects)
        scored = worker_module.score(prospects, notes)
        drafts, failures, _, _ = worker_module.write_drafts(scored, notes, False)

        self.assertEqual(len(drafts), 2)
        self.assertEqual(len(failures), 0)
        self.assertTrue(all(row["fit_score"] >= 75 for row in scored))
        self.assertTrue(all(row["evidence_strength"] >= 0.8 for row in scored))
        self.assertTrue(all(len(row["evidence_points"]) >= 2 for row in scored))
        self.assertTrue(all(row["validation_status"] == "send_ready" for row in drafts))
        for prospect in prospects:
            self.assertIn("canonical_entity_id", prospect)
            self.assertTrue(any(entity["entity_type"] == "project" for entity in prospect["canonical_entities"]))
            self.assertTrue(any(rel["relationship_type"] == "person_reachable_for_project" for rel in prospect["verified_relationships"]))
        for note in notes:
            self.assertGreaterEqual(len(note["evidence"]), 4)
            self.assertTrue(all(item["clean"] for item in note["evidence"]))
        for row in scored:
            evidence_ids = {item["evidence_id"] for item in row["evidence"]}
            referenced = {item for ids in row["score_evidence_ids"].values() for item in ids}
            self.assertTrue(referenced)
            self.assertTrue(referenced.issubset(evidence_ids))

    def test_canonical_entity_graph_keeps_domain_conflicts_separate(self):
        prospect = worker_module.normalize_prospect(
            {
                "prospect_id": "conflict",
                "name": "Avery Founder",
                "email": "avery@founder.dev",
                "email_source_url": "https://founder.dev/contact",
                "project": "Avery AI",
                "project_url": "https://avery-ai.example/product",
                "project_description": "AI inference workers with retries and artifacts.",
                "research_text": "AI inference workers run background jobs with retries and artifacts.",
                "owner_login": "avery-ai",
                "owner_type": "Organization",
                "updated_at": worker_module.utc_now(),
            }
        )
        diagnostics = worker_module.qualification_diagnostics(prospect)
        evidence = worker_module.structured_evidence_for_prospect(
            prospect,
            diagnostics,
            diagnostics["evidence_points"],
            diagnostics["pain_signals"],
        )
        graph = worker_module.canonical_entity_graph(prospect, evidence)
        self.assertTrue(graph["conflicting_claims"])
        self.assertNotEqual(
            [entity for entity in graph["canonical_entities"] if entity["entity_type"] == "domain"][0]["canonical_name"],
            "avery-ai.example",
        )

    def test_generic_queue_package_is_excluded_and_capped(self):
        now = datetime.utcnow().isoformat() + "Z"
        queue = worker_module.normalize_prospect(
            {
                "prospect_id": "queue",
                "name": "Queue Maintainer",
                "email": "maintainer@yocto-queue.dev",
                "email_source_url": "https://yocto-queue.dev/contact",
                "project": "sindresorhus/yocto-queue",
                "project_url": "https://github.com/sindresorhus/yocto-queue",
                "project_description": "Tiny queue data structure.",
                "research_text": "Tiny queue data structure for JavaScript. A generic utility collection with enqueue and dequeue helpers.",
                "owner_login": "sindresorhus",
                "owner_type": "User",
                "updated_at": now,
            }
        )
        diagnostics = worker_module.qualification_diagnostics(queue)
        self.assertEqual(diagnostics["exclusion_rule_triggered"], "generic_package_without_ai_workload")
        queue["diagnostics"] = {**diagnostics, "excluded": False, "evidence_points": ["queue data structure"], "contact_quality": 8}
        note = {
            "prospect_id": "queue",
            "summary": "Generic queue utility.",
            "personalization_detail": "queue data structure",
            "junglegrid_relevance": "No direct AI workload.",
            "evidence_urls": queue["evidence_urls"],
            "evidence_strength": 0.3,
            "evidence_points": ["queue data structure"],
            "pain_signals": [],
        }
        scored = worker_module.score([queue], [note])[0]
        self.assertLessEqual(scored["fit_score"], 50)

    def test_contaminated_evidence_rejected(self):
        contaminated = "Home Docs Pricing @keyframes spin { transform: rotate(1turn); } [npm-image] min-height: 100vh"
        self.assertFalse(worker_module.is_clean_evidence_text(contaminated))
        self.assertIn("css_keyframes", worker_module.contamination_reasons(contaminated))

    def test_source_registry_reports_disabled_and_missing_credentials(self):
        with patch.dict(os.environ, {"YOUTUBE_API_KEY": "", "DISCORD_BOT_TOKEN": ""}, clear=False):
            registry = build_default_registry({"sources": {"discord": {"enabled": True}, "youtube": {"enabled": True}}})
        statuses = {status.source_type: status for status in registry.health()}
        self.assertEqual(statuses["github"].status, "healthy")
        self.assertEqual(statuses["discord"].status, "disabled")
        self.assertIn("missing_credentials", statuses["discord"].reason)
        self.assertEqual(statuses["youtube"].status, "disabled")

    def test_adapter_enforces_retry_policy_and_returns_structured_redacted_errors(self):
        registry = build_default_registry(
            {
                "sources": {
                    "npm": {
                        "enabled": True,
                        "retry_count": 2,
                        "rate_limit_per_minute": 100000,
                    }
                }
            }
        )
        adapter = registry.get("npm")
        with patch.object(
            adapter,
            "_discover_npm",
            side_effect=urllib.error.URLError("https://api.test?q=1&api_key=secret-value"),
        ) as discover:
            with patch("workers.outreach.source_adapters.time.sleep"):
                candidates = adapter.discover(
                    DiscoveryQuery(text="test", limit=1),
                    DiscoveryContext(deterministic=False),
                )
        self.assertEqual(candidates, [])
        self.assertEqual(discover.call_count, 3)
        errors = adapter.drain_errors()
        self.assertEqual([error.attempt for error in errors], [1, 2, 3])
        self.assertTrue(all(error.retryable for error in errors))
        self.assertNotIn("secret-value", " ".join(error.message for error in errors))
        self.assertEqual(adapter.capabilities.retry_count, 2)
        self.assertEqual(adapter.health_check().status, "degraded")

    def test_adapter_caches_discovery_results_with_ttl(self):
        registry = build_default_registry(
            {
                "sources": {
                    "npm": {
                        "enabled": True,
                        "cache_ttl_seconds": 60,
                        "rate_limit_per_minute": 100000,
                    }
                }
            }
        )
        adapter = registry.get("npm")
        candidate = SourceCandidate(
            source_type="npm",
            source_id="npm:cached",
            url="https://www.npmjs.com/package/cached",
            title="cached",
        )
        with patch.object(adapter, "_discover_npm", return_value=[candidate]) as discover:
            first = adapter.discover(
                DiscoveryQuery(text="cached", limit=1),
                DiscoveryContext(deterministic=False),
            )
            second = adapter.discover(
                DiscoveryQuery(text="cached", limit=1),
                DiscoveryContext(deterministic=False),
            )
        self.assertEqual(first, second)
        self.assertEqual(discover.call_count, 1)

    def test_public_adapter_fixture_discovery_and_normalization(self):
        registry = build_default_registry({"sources": {"npm": {"enabled": True}}})
        adapter = registry.get("npm")
        self.assertIsNotNone(adapter)
        context = DiscoveryContext(deterministic=True)
        candidates = adapter.discover(DiscoveryQuery(text="batch inference workers", limit=1), context)
        self.assertEqual(len(candidates), 1)
        documents = adapter.fetch(candidates[0], context)
        evidence = adapter.normalize(documents, context)
        self.assertGreaterEqual(len(evidence), 1)
        self.assertEqual(evidence[0].source_type, "npm")
        self.assertIn(evidence[0].claim_type, {"ai_workload", "infrastructure_pain"})

    def test_public_adapter_extracts_repository_and_official_links(self):
        registry = build_default_registry({"sources": {"hackernews": {"enabled": True}}})
        adapter = registry.get("hackernews")
        candidate = SourceCandidate(
            source_type="hackernews",
            source_id="hn:1",
            url="https://news.ycombinator.com/item?id=1",
            title="Show HN: Agent Runtime",
            metadata={
                "content": "Agent Runtime runs batch inference workers. Repo https://github.com/acme/agent-runtime docs https://agent-runtime.dev",
                "retrieval_method": "api",
            },
        )
        enriched = adapter.enrich_candidate(candidate)
        self.assertEqual(enriched.metadata["repository_url"], "https://github.com/acme/agent-runtime")
        self.assertEqual(enriched.metadata["official_url"], "https://agent-runtime.dev")

    def test_syndicated_documents_share_independence_group(self):
        registry = build_default_registry({"sources": {"news_rss": {"enabled": True}}})
        adapter = registry.get("news_rss")
        now = worker_module.utc_now()
        documents = [
            RawSourceDocument(
                source_type="news_rss",
                source_id="news:1",
                source_url="https://news.example/a",
                retrieval_method="feed",
                retrieved_at=now,
                content="Agent Runtime launches batch inference workers with retries and artifacts.",
                metadata={"canonical_event_id": "agent-runtime-launch"},
            ),
            RawSourceDocument(
                source_type="news_rss",
                source_id="news:2",
                source_url="https://mirror.example/a",
                retrieval_method="feed",
                retrieved_at=now,
                content="Agent Runtime launches batch inference workers with retries and artifacts.",
                metadata={"canonical_event_id": "agent-runtime-launch"},
            ),
        ]
        evidence = adapter.normalize(documents, DiscoveryContext(deterministic=True))
        self.assertGreaterEqual(len(evidence), 2)
        self.assertEqual(len({item.independence_group for item in evidence}), 1)

    def test_public_adapter_rejects_contaminated_and_generic_package_evidence(self):
        registry = build_default_registry({"sources": {"npm": {"enabled": True}}})
        adapter = registry.get("npm")
        now = worker_module.utc_now()
        contaminated = RawSourceDocument(
            source_type="npm",
            source_id="npm:bad-css",
            source_url="https://www.npmjs.com/package/bad-css",
            retrieval_method="api",
            retrieved_at=now,
            content="@keyframes spin { transform: rotate(1turn); } queue data structure",
        )
        generic = RawSourceDocument(
            source_type="npm",
            source_id="npm:yocto-queue",
            source_url="https://www.npmjs.com/package/yocto-queue",
            retrieval_method="api",
            retrieved_at=now,
            content="Tiny queue data structure for JavaScript utilities.",
        )
        self.assertEqual(adapter.normalize([contaminated, generic], DiscoveryContext(deterministic=True)), [])

    def test_adapter_candidates_can_feed_prospect_discovery_through_repository_resolution(self):
        class FakeAdapter:
            source_type = "npm"

            def health_check(self):
                return SourceHealth("npm", "healthy", "test")

            def discover(self, query, context):
                return [
                    SourceCandidate(
                        source_type="npm",
                        source_id="npm:agent-runtime",
                        url="https://www.npmjs.com/package/agent-runtime",
                        title="agent-runtime",
                        metadata={
                            "repository_url": "https://github.com/sample/agent-runtime",
                            "content": "Agent runtime launches batch inference workers with retries and artifacts.",
                        },
                    )
                ]

            def fetch(self, candidate, context):
                return [
                    RawSourceDocument(
                        source_type="npm",
                        source_id=candidate.source_id,
                        source_url=candidate.url,
                        retrieval_method="api",
                        retrieved_at=worker_module.utc_now(),
                        content=candidate.metadata["content"],
                    )
                ]

            def normalize(self, documents, context):
                return build_default_registry({"sources": {"npm": {"enabled": True}}}).get("npm").normalize(documents, context)

        class FakeRegistry:
            def enabled(self):
                return [FakeAdapter()]

        expected = worker_module.normalize_prospect(
            {
                "prospect_id": "adapter-prospect",
                "name": "Avery Maintainer",
                "email": "avery@agent-runtime.dev",
                "email_source_url": "https://agent-runtime.dev/contact",
                "project": "sample/agent-runtime",
                "project_url": "https://github.com/sample/agent-runtime",
                "project_description": "Agent runtime launches batch inference workers.",
                "research_text": "Agent runtime launches batch inference workers with retries and artifacts.",
                "owner_login": "sample",
                "owner_type": "User",
                "updated_at": worker_module.utc_now(),
            }
        )
        with patch.object(worker_module, "request_json", return_value={"full_name": "sample/agent-runtime"}):
            with patch.object(worker_module, "repo_to_prospect", return_value=expected):
                prospects, signals = worker_module.discover_from_adapters(FakeRegistry(), 1, None, set())
        self.assertEqual(len(prospects), 1)
        self.assertEqual(prospects[0]["discovery_source"], "npm")
        self.assertEqual(signals[0]["evidence_count"], 1)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Jungle Grid outreach research and draft artifact worker."""

from __future__ import annotations

import argparse
import http.client
import hashlib
import json
import logging
import os
import random
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any

import yaml

try:
    from workers.outreach.source_adapters import (
        DiscoveryContext,
        DiscoveryQuery,
        ProspectSourceAdapter,
        SourceCandidate,
        SourceHealth,
        build_default_registry,
    )
except ModuleNotFoundError:  # pragma: no cover - supports direct script execution
    from source_adapters import (
        DiscoveryContext,
        DiscoveryQuery,
        ProspectSourceAdapter,
        SourceCandidate,
        SourceHealth,
        build_default_registry,
    )

SITE = "https://junglegrid.dev"
ALLOWED_LINKS = [SITE]
MIN_WORDS = 70
MAX_WORDS = 140
MAX_SUBJECT = 79
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
URL_RE = re.compile(r"https?://[^\s<>\"')\]]+")
HREF_RE = re.compile(r'href=["\']([^"\'#]+)["\']', re.I)
FORM_ACTION_RE = re.compile(r'<form\b[^>]*action=["\']([^"\']+)["\']', re.I)
CONTACT_CONTEXT_RE = re.compile(
    r"\b(contact|business|partnerships?|inquiries|reach(?:\s+us)?|email|support|hello)\b",
    re.I,
)
LIKELY_DOCS_PATH_RE = re.compile(
    r"(?:^|/)(docs?|guide|developers?|api|reference|manual)(?:/|$)",
    re.I,
)
LIKELY_CONTACT_PATH_RE = re.compile(
    r"(?:^|/)(contact|about|team|company|support|docs|legal|privacy|impressum)(?:/|$)",
    re.I,
)
TARGET_TERMS = re.compile(
    r"\b(agent|agentic|mcp|workflow|inference|training|fine[- ]?tun|gpu|batch|"
    r"runtime|compute|orchestrat|model serving|tool calling)\b",
    re.I,
)
CONCRETE_WORKLOAD_RE = re.compile(
    r"\b(mcp|model context protocol|agent(?:ic)?|llm|rag|evals?|evaluation|inference|fine[- ]?tun\w*|"
    r"gpu|cuda|batch|distributed workers?|background jobs?|long[- ]running|deployment|latency|cost|scal\w*|"
    r"serverless gpu|model serving|vllm|ollama|qwen|runpod|modal|replicate|artifacts?|retries?|"
    r"tool calling|orchestrat\w*|scheduler|scrap\w*|enrichment)\b",
    re.I,
)
DIRECT_AI_WORKLOAD_RE = re.compile(
    r"\b(model serving|inference|training|fine[- ]?tun\w*|gpu|cuda|vllm|ollama|qwen|rag|evals?|"
    r"llm|mcp|model context protocol|tool calling|agentic|ai agent|multi-agent|batch ai|"
    r"multimodal|serverless gpu)\b",
    re.I,
)
EXECUTION_SURFACE_RE = re.compile(
    r"\b(long[- ]running|background jobs?|distributed workers?|worker jobs?|batch|retries?|job state|"
    r"logs?|artifacts?|queue(?:s|ing)?|orchestrat\w*|scheduler|compute routing|capacity|"
    r"timeout|startup|memory|scal\w*|deployment)\b",
    re.I,
)
GENERIC_PACKAGE_RE = re.compile(
    r"\b(queue data structure|microtask|polyfill|shim|ponyfill|collection|algorithm|utility|utilities|"
    r"browser utility|wrapper|tiny queue|priority queue|data structures?|event emitter|promise queue)\b",
    re.I,
)
CONTROL_PLANE_RE = re.compile(
    r"\b(control plane|orchestrat\w+ layer|workflow engine|scheduler|queue manager|job controller|"
    r"durable executor|worker fleet)\b",
    re.I,
)
CONTAMINATION_RE = re.compile(
    r"(@keyframes|data-astro|transform:|min-height:|\[ci-image\]|\[npm-image\]|"
    r"<(?:script|style|svg)\b|</(?:script|style|svg)>|^\s*[.#][a-z0-9_-]+\s*\{|"
    r"(?:display|position|padding|margin|font-size|background|border-radius)\s*:)",
    re.I | re.M,
)
VAGUE_RELEVANCE_RE = re.compile(r"\b(agent|ai|workflow|automation)\b", re.I)
NON_AI_AGENT_RE = re.compile(
    r"\b(global-agent|user-agent|http agent|https agent|proxy agent|ssh agent|browser agent|agent forwarding)\b",
    re.I,
)
GENERIC_EMAIL_LOCAL_PARTS = {
    "opensource",
    "security",
    "support",
    "noreply",
    "no-reply",
    "privacy",
    "legal",
    "info",
    "contact",
    "admin",
}
GENERIC_TEAM_EMAIL_LOCAL_PARTS = {"team", "hello", "hi", "founders", "office"}
LARGE_VENDOR_ORGS = {
    "microsoft",
    "aws",
    "amazon",
    "google",
    "meta",
    "nvidia",
    "huggingface",
    "langchain-ai",
    "openai",
    "vercel",
    "cloudflare",
}
LARGE_FOUNDATION_ORGS = {
    "apache",
    "mozilla",
    "linuxfoundation",
    "kubernetes",
    "pytorch",
}
REPO_TYPE_EXCLUSION_RE = re.compile(
    r"\b(sdk|docs?-only|documentation|examples?|starter|template|boilerplate|showcase|landing page|"
    r"ui kit|component library|design system|static site|website|awesome list)\b",
    re.I,
)
PAIN_SIGNAL_RE = re.compile(
    r"\b(background jobs?|workers?|queues?|gpu|latency|cost|scal\w*|deploy(?:ment|ing)?|hosting|"
    r"performance|retries?|batch|fine[- ]?tun\w*|evals?|artifacts?|long[- ]running|inference)\b",
    re.I,
)
JOBS = {
    "discover",
    "research",
    "score",
    "write-emails-template",
    "write-emails-qwen",
    "full-run-template",
    "full-run-qwen",
    "conversation-turn-qwen",
}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("outreach-worker")
OLLAMA_PROCESS: subprocess.Popen[Any] | None = None
CAMPAIGN: dict[str, Any] = {
    "schemaVersion": "1.0",
    "workspaceId": "default",
    "campaignId": "jungle-grid",
    "name": "Jungle Grid AI execution",
    "offer": {
        "name": "Jungle Grid",
        "description": "Durable managed execution for inference, workers, retries, logs, and artifacts.",
        "url": SITE,
        "senderName": "Benedict",
        "signature": "Benedict",
    },
    "idealCustomerProfile": {
        "description": "Builders operating AI and durable background workloads.",
        "categories": [
            "agent_framework",
            "mcp",
            "ai_infrastructure",
            "inference_training",
            "agent_compute",
            "workflow_automation",
            "llm_application",
            "open_source_ai",
        ],
        "targetTerms": ["ai", "agent", "llm", "mcp", "inference", "training", "gpu"],
        "workloadTerms": ["inference", "training", "gpu", "llm", "agentic", "batch ai"],
        "executionTerms": ["worker", "background job", "long-running", "retry", "logs", "artifacts", "queue"],
        "painTerms": ["timeout", "latency", "capacity", "memory", "cost", "scale", "deployment"],
        "exclusionTerms": ["queue data structure", "microtask", "polyfill", "shim", "browser utility"],
    },
    "qualification": {
        "requireTargetSignal": True,
        "requireWorkloadSignal": True,
        "requireExecutionSignal": True,
        "requirePainSignal": False,
        "maximumActivityAgeDays": 180,
    },
    "messaging": {
        "positioning": "Jungle Grid can provide the durable execution layer behind those workloads, including retries, logs, and artifacts.",
        "callToAction": "If that is a live problem for you, the shortest overview is",
        "subjectPrefix": "Jungle Grid and",
    },
    "execution": {
        "researchModel": "qwen2.5:3b",
        "scoringModel": "qwen2.5:3b",
        "draftingModel": "qwen2.5:3b",
        "validationModel": "qwen2.5:3b",
    },
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def word_count(value: str) -> int:
    return len(value.strip().split())


def clip_words(value: str, limit: int) -> str:
    return " ".join(value.replace("\n", " ").split()[:limit]).rstrip(" ,:;.-")


def normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def split_sentences(value: str) -> list[str]:
    return [sentence.strip() for sentence in re.split(r"(?<=[.!?])\s+|\n+", value) if sentence.strip()]


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def days_since(value: str | None) -> int | None:
    parsed = parse_iso_datetime(value)
    if parsed is None:
        return None
    delta = datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)
    return max(0, int(delta.total_seconds() // 86400))


def bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def source_registry_config() -> dict[str, Any]:
    configured_path = os.getenv("OUTREACH_SOURCES_CONFIG", "").strip()
    candidates = [
        Path(configured_path) if configured_path else None,
        Path("config/sources.yaml"),
        Path(__file__).resolve().parents[2] / "config" / "sources.yaml",
        Path("/app/config/sources.yaml"),
    ]
    config: dict[str, Any] = {}
    for candidate in candidates:
        if candidate is None or not candidate.is_file():
            continue
        loaded = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
        if not isinstance(loaded, dict):
            raise ValueError(f"Source configuration must be a mapping: {candidate}")
        config = loaded
        break

    restricted = config.setdefault("restricted_sources", {})
    if not isinstance(restricted, dict):
        raise ValueError("restricted_sources must be a mapping")
    for source_type, env_name in {
        "discord": "ENABLE_DISCORD_SOURCE",
        "slack": "ENABLE_SLACK_SOURCE",
        "linkedin": "ENABLE_LINKEDIN_ENRICHMENT",
        "facebook_page": "ENABLE_FACEBOOK_PAGE_ENRICHMENT",
        "product_hunt": "ENABLE_PRODUCT_HUNT_SOURCE",
    }.items():
        source_config = restricted.setdefault(source_type, {})
        if not isinstance(source_config, dict):
            raise ValueError(f"restricted_sources.{source_type} must be a mapping")
        if env_name in os.environ:
            source_config["enabled"] = bool_env(env_name)
    return config


def _terms_regex(terms: list[str]) -> re.Pattern[str]:
    escaped = [re.escape(term.strip()).replace(r"\ ", r"\s+") for term in terms if term.strip()]
    return re.compile(rf"\b(?:{'|'.join(escaped)})\b", re.I) if escaped else re.compile(r"(?!x)x")


def campaign_regex(key: str) -> re.Pattern[str]:
    return _terms_regex([str(term) for term in CAMPAIGN["idealCustomerProfile"].get(key, [])])


def configure_campaign(input_path: Path | None) -> dict[str, Any]:
    global CAMPAIGN, SITE, ALLOWED_LINKS
    raw = os.getenv("OUTREACH_CAMPAIGN_CONFIG", "").strip()
    contract_raw = os.getenv("OUTREACH_JOB_CONTRACT", "").strip()
    payload: Any = None
    if raw:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as error:
            raise ValueError("OUTREACH_CAMPAIGN_CONFIG must contain valid JSON.") from error
    elif contract_raw:
        try:
            contract = json.loads(contract_raw)
        except json.JSONDecodeError as error:
            raise ValueError("OUTREACH_JOB_CONTRACT must contain valid JSON.") from error
        if contract.get("schema_version") != "1.0":
            raise ValueError("Jungle Grid job contract must use schema_version 1.0.")
        payload = contract.get("campaign_configuration")
    elif input_path and input_path.exists():
        source = json.loads(input_path.read_text(encoding="utf-8"))
        if isinstance(source, dict):
            payload = source.get("campaign_configuration")
    if payload is not None:
        if not isinstance(payload, dict) or payload.get("schemaVersion") != "1.0":
            raise ValueError("Campaign configuration must use schemaVersion 1.0.")
        CAMPAIGN = payload
    SITE = str(CAMPAIGN["offer"]["url"])
    ALLOWED_LINKS = [SITE]
    return CAMPAIGN


def dedupe_store_path() -> Path:
    raw = os.getenv("OUTREACH_MEMORY_PATH", "data/outreach/prospect_memory.json")
    return Path(raw)


def clean_research_text(value: str) -> str:
    text = unescape(value or "")
    text = re.sub(r"<(?:script|style|svg)\b.*?</(?:script|style|svg)>", " ", text, flags=re.I | re.S)
    text = re.sub(r"@keyframes\b.*?(?:}\s*})", " ", text, flags=re.I | re.S)
    text = re.sub(r"\bdata-astro-[a-z0-9_-]+(?:=\"[^\"]*\")?", " ", text, flags=re.I)
    text = re.sub(
        r"^\s*(?:transform|min-height|display|position|padding|margin|font-size|background|border-radius)\s*:\s*[^;]+;?\s*$",
        " ",
        text,
        flags=re.I | re.M,
    )
    text = re.sub(r"<!--.*?-->", " ", text, flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"!\[[^\]]*]\([^)]*\)", " ", text)
    text = re.sub(r"\[[^\]]+]\([^)]*(?:shields\.io|badge|img\.shields)[^)]*\)", " ", text, flags=re.I)
    text = re.sub(r"^\s*\[[^\]]*(?:ci|npm|badge|image|version)[^\]]*]:\s*\S+\s*$", " ", text, flags=re.I | re.M)
    text = re.sub(r"^\s*\[[^\]]+]:\s*\S+\s*$", " ", text, flags=re.M)
    text = re.sub(r"^\s*[-*]\s+\[[^]]+\]\([^)]*\)\s*$", " ", text, flags=re.M)
    text = re.sub(r"^\s*(table of contents|toc|navigation|contents)\s*$", " ", text, flags=re.I | re.M)
    text = re.sub(r"^\s*#{1,6}\s*(table of contents|toc|navigation|contents).*$", " ", text, flags=re.I | re.M)
    text = re.sub(r"\[(.*?)\]\([^)]*\)", r"\1", text)
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = re.sub(r"[`*_>#|]", " ", raw_line).strip()
        if not line:
            continue
        lowered = normalize_name(line)
        if not lowered:
            continue
        if lowered in {"table of contents", "toc", "contents", "navigation"}:
            continue
        if CONTAMINATION_RE.search(line):
            continue
        if len(line) < 3:
            continue
        lines.append(line)
    return re.sub(r"\s+", " ", " ".join(lines)).strip()


def contamination_reasons(value: str) -> list[str]:
    reasons: list[str] = []
    checks = {
        "css_keyframes": r"@keyframes",
        "astro_attribute": r"data-astro",
        "css_transform": r"transform:",
        "css_min_height": r"min-height:",
        "badge_reference": r"\[(?:ci|npm)-image\]",
        "style_or_svg_markup": r"<(?:style|script|svg)\b",
        "css_declaration": r"(?:display|position|padding|margin|font-size|background|border-radius)\s*:",
        "malformed_markup": r"</?[a-z][^>]{80,}|[{}]{4,}",
    }
    for reason, pattern in checks.items():
        if re.search(pattern, value or "", re.I):
            reasons.append(reason)
    nav_tokens = re.findall(r"\b(?:home|docs|pricing|blog|login|sign up|features|contact)\b", value or "", re.I)
    if len(nav_tokens) >= 8:
        reasons.append("repeated_navigation")
    punctuation = re.findall(r"[|_\-*/#]{4,}", value or "")
    if len(punctuation) >= 4:
        reasons.append("excessive_punctuation")
    return sorted(set(reasons))


def is_clean_evidence_text(value: str) -> bool:
    cleaned = clean_research_text(value)
    return bool(cleaned and len(cleaned) >= 20 and not contamination_reasons(value) and not contamination_reasons(cleaned))


def unique_preserve_order(values: list[str]) -> list[str]:
    return list(dict.fromkeys([value for value in values if value]))


def contact_quality(email: str, source_type: str, context: str) -> int:
    local = email.partition("@")[0].lower()
    score = {
        "official_website": 8,
        "github_profile": 9,
        "project_docs": 7,
        "package_page": 6,
        "repository_readme": 6,
    }.get(source_type, 5)
    if local in GENERIC_EMAIL_LOCAL_PARTS:
        score -= 5
    elif local in GENERIC_TEAM_EMAIL_LOCAL_PARTS:
        score -= 2
    if CONTACT_CONTEXT_RE.search(context):
        score += 1
    return max(0, min(10, score))


def is_generic_contact_email(email: str) -> bool:
    return email.partition("@")[0].lower() in GENERIC_EMAIL_LOCAL_PARTS


def is_team_email(email: str) -> bool:
    return email.partition("@")[0].lower() in GENERIC_TEAM_EMAIL_LOCAL_PARTS


def is_large_org(owner_login: str) -> bool:
    normalized = normalize_name(owner_login).replace(" ", "")
    return normalized in LARGE_VENDOR_ORGS or normalized in LARGE_FOUNDATION_ORGS


def owner_key(owner_login: str) -> str:
    return normalize_name(owner_login).replace(" ", "")


def disambiguates_ai_agent(text: str) -> bool:
    lowered = text.lower()
    if NON_AI_AGENT_RE.search(lowered):
        return False
    if "agent" not in lowered:
        return False
    return bool(
        re.search(
            r"\b(ai|llm|autonomous|mcp|tool calling|task execution|workflow agent|agent runtime|"
            r"multi-agent|crewai|langgraph|openai agents?|background worker|long-running jobs?)\b",
            lowered,
        )
    )


def extract_evidence_points(text: str) -> list[str]:
    evidence: list[str] = []
    target_re = campaign_regex("targetTerms")
    workload_re = campaign_regex("workloadTerms")
    execution_re = campaign_regex("executionTerms")
    for sentence in split_sentences(text):
        if not is_clean_evidence_text(sentence):
            continue
        for chunk in re.split(r",|;|\band\b", sentence):
            lowered = chunk.lower().strip()
            if NON_AI_AGENT_RE.search(lowered):
                continue
            if GENERIC_PACKAGE_RE.search(lowered) and not workload_re.search(lowered):
                continue
            if not (target_re.search(lowered) or workload_re.search(lowered) or execution_re.search(lowered)):
                continue
            if re.search(r"\b(queue|worker|agent)\b", lowered) and not (
                workload_re.search(lowered) or execution_re.search(lowered)
            ):
                continue
            cleaned = clip_words(chunk.strip(), 18)
            if cleaned and cleaned not in evidence:
                evidence.append(cleaned)
            if len(evidence) == 3:
                return evidence
    return evidence


def extract_pain_signals(text: str) -> list[str]:
    signals: list[str] = []
    pain_re = campaign_regex("painTerms")
    workload_re = campaign_regex("workloadTerms")
    execution_re = campaign_regex("executionTerms")
    for sentence in split_sentences(text):
        if not is_clean_evidence_text(sentence):
            continue
        if not pain_re.search(sentence):
            continue
        if not (workload_re.search(sentence) or execution_re.search(sentence)):
            continue
        cleaned = clip_words(sentence, 24)
        if cleaned and cleaned not in signals:
            signals.append(cleaned)
    return signals[:3]


def evidence_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def source_authority(source_type: str) -> float:
    return {
        "official_product_documentation": 1.0,
        "project_docs": 1.0,
        "repository_readme": 0.95,
        "github_repository": 0.95,
        "gitlab_repository": 0.95,
        "official_website": 0.9,
        "maintainer_community_statement": 0.85,
        "news_rss": 0.75,
        "hackernews": 0.6,
        "reddit": 0.45,
        "facebook_page": 0.55,
        "search_result": 0.2,
        "unverified_repost": 0.1,
    }.get(source_type, 0.5)


def evidence_directness(claim_type: str, claim: str, source_type: str) -> str:
    if source_type in {"project_docs", "repository_readme", "github_repository", "gitlab_repository", "official_website"}:
        return "direct" if claim_type in {"ai_workload", "integration_surface", "activity", "contact", "role"} else "strong_inference"
    if claim_type in {"why_now", "infrastructure_pain"}:
        return "strong_inference"
    return "weak_inference"


def build_evidence(
    prospect: dict[str, Any],
    claim_type: str,
    claim: str,
    source_url: str,
    source_type: str,
    directness: str | None = None,
    published_at: str | None = None,
) -> dict[str, Any] | None:
    cleaned = clean_research_text(claim)
    if not is_clean_evidence_text(cleaned):
        return None
    entity_id = prospect.get("entity_id") or f"project:{prospect.get('project_key') or normalize_name(prospect['project']).replace(' ', '-')}"
    content_hash = evidence_hash(f"{entity_id}|{claim_type}|{cleaned}|{source_url}")
    return {
        "evidence_id": f"ev_{content_hash}",
        "entity_id": entity_id,
        "claim_type": claim_type,
        "claim": clip_words(cleaned, 28),
        "source_url": source_url,
        "source_type": source_type,
        "source_authority": source_authority(source_type),
        "published_at": published_at,
        "retrieved_at": utc_now(),
        "directness": directness or evidence_directness(claim_type, cleaned, source_type),
        "freshness": 1.0,
        "independence_group": evidence_hash(source_url.split("#", 1)[0].lower()),
        "content_hash": content_hash,
        "clean": True,
    }


def evidence_by_type(evidence: list[dict[str, Any]], claim_type: str) -> list[dict[str, Any]]:
    return [item for item in evidence if item.get("claim_type") == claim_type and item.get("clean")]


def evidence_strength_bucket(evidence: list[dict[str, Any]]) -> int:
    return len({item["independence_group"] for item in evidence if item.get("clean")})


def canonical_id(entity_type: str, value: str) -> str:
    normalized = normalize_name(value).replace(" ", "-") or evidence_hash(value)
    return f"{entity_type}:{normalized}"


def domain_from_url(url: str) -> str:
    try:
        return (urllib.parse.urlparse(url).hostname or "").lower().removeprefix("www.")
    except ValueError:
        return ""


def canonical_entity_graph(prospect: dict[str, Any], evidence: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    evidence = evidence or []
    email = prospect.get("email", "").lower()
    email_domain = email.partition("@")[2]
    project = prospect.get("project", "")
    project_url = prospect.get("project_url", "")
    project_domain = domain_from_url(project_url)
    owner = prospect.get("owner_login", "")
    repo = parse_github_repo(project_url)
    project_id = prospect.get("entity_id") or canonical_id("project", project)
    company_id = canonical_id("company", owner or email_domain or project)
    person_id = canonical_id("person", f"{prospect.get('name', '')} {email}")
    primary_contact = next(iter(prospect.get("contact_points", [])), {})
    contact_value = email or str(primary_contact.get("value", ""))
    contact_id = canonical_id("contact", contact_value)
    domain_id = canonical_id("domain", email_domain or project_domain or project)
    entities: list[dict[str, Any]] = [
        {
            "entity_id": project_id,
            "entity_type": "project",
            "canonical_name": project,
            "aliases": unique_preserve_order([project, prospect.get("name", ""), project.rsplit("/", 1)[-1] if "/" in project else ""]),
            "source_specific_ids": {"project_url": project_url},
            "confidence": 0.9,
        },
        {
            "entity_id": person_id,
            "entity_type": "person",
            "canonical_name": prospect.get("name", ""),
            "aliases": unique_preserve_order([prospect.get("name", ""), owner if prospect.get("owner_type", "").lower() == "user" else ""]),
            "source_specific_ids": {"email": email},
            "confidence": 0.75 if prospect.get("owner_type", "").lower() == "user" else 0.55,
        },
        {
            "entity_id": contact_id,
            "entity_type": "contact_point",
            "canonical_name": contact_value,
            "aliases": [contact_value],
            "source_specific_ids": {
                "contact_type": str(primary_contact.get("type", "email" if email else ""))
            },
            "confidence": float((prospect.get("contact_provenance") or {}).get("confidence") or 0.5),
        },
        {
            "entity_id": domain_id,
            "entity_type": "domain",
            "canonical_name": email_domain or project_domain,
            "aliases": unique_preserve_order([email_domain, project_domain]),
            "source_specific_ids": {},
            "confidence": 0.65,
        },
        {
            "entity_id": company_id,
            "entity_type": "company",
            "canonical_name": owner or email_domain or project,
            "aliases": unique_preserve_order([owner, email_domain]),
            "source_specific_ids": {"owner_login": owner},
            "confidence": 0.6,
        },
    ]
    relationships: list[dict[str, Any]] = [
        {
            "relationship_type": "person_reachable_for_project",
            "from_entity_id": person_id,
            "to_entity_id": project_id,
            "confidence": 0.85 if prospect.get("owner_type", "").lower() == "user" else 0.6,
            "evidence_ids": [item["evidence_id"] for item in evidence if item.get("claim_type") in {"role", "contact"}],
        },
        {
            "relationship_type": "contact_point_for_person_or_project",
            "from_entity_id": contact_id,
            "to_entity_id": person_id if prospect.get("owner_type", "").lower() == "user" else project_id,
            "confidence": float((prospect.get("contact_provenance") or {}).get("confidence") or 0.5),
            "evidence_ids": [item["evidence_id"] for item in evidence if item.get("claim_type") == "contact"],
        },
        {
            "relationship_type": "project_has_domain",
            "from_entity_id": project_id,
            "to_entity_id": domain_id,
            "confidence": 0.65,
            "evidence_ids": [],
        },
        {
            "relationship_type": "company_or_owner_controls_project",
            "from_entity_id": company_id,
            "to_entity_id": project_id,
            "confidence": 0.75 if owner and owner.lower() in project.lower() else 0.5,
            "evidence_ids": [],
        },
    ]
    if repo:
        repo_id = canonical_id("repository", f"github.com/{repo[0]}/{repo[1]}")
        entities.append(
            {
                "entity_id": repo_id,
                "entity_type": "repository",
                "canonical_name": f"{repo[0]}/{repo[1]}",
                "aliases": [f"github.com/{repo[0]}/{repo[1]}", f"{repo[0]}/{repo[1]}"],
                "source_specific_ids": {"github": f"{repo[0]}/{repo[1]}"},
                "confidence": 0.95,
            }
        )
        relationships.append(
            {
                "relationship_type": "repository_represents_project",
                "from_entity_id": repo_id,
                "to_entity_id": project_id,
                "confidence": 0.95,
                "evidence_ids": [item["evidence_id"] for item in evidence if item.get("source_type") in {"repository_readme", "github_repository"}],
            }
        )
    for url in prospect.get("evidence_urls", []):
        document_id = canonical_id("source_document", url)
        entities.append(
            {
                "entity_id": document_id,
                "entity_type": "source_document",
                "canonical_name": url,
                "aliases": [url],
                "source_specific_ids": {"url": url},
                "confidence": 1.0,
            }
        )
        relationships.append(
            {
                "relationship_type": "source_document_supports_project",
                "from_entity_id": document_id,
                "to_entity_id": project_id,
                "confidence": 0.7,
                "evidence_ids": [item["evidence_id"] for item in evidence if item.get("source_url") == url],
            }
        )
    conflicts: list[dict[str, Any]] = []
    if email_domain and project_domain and email_domain != project_domain and "github.com" not in project_domain:
        conflicts.append(
            {
                "claim": "contact email domain differs from project domain",
                "values": [email_domain, project_domain],
                "resolution": "kept_separate_pending_stronger_official_link",
                "confidence": 0.4,
            }
        )
    return {
        "canonical_entity_id": project_id,
        "canonical_entities": entities,
        "verified_relationships": relationships,
        "conflicting_claims": conflicts,
    }


def structured_evidence_for_prospect(
    prospect: dict[str, Any],
    diagnostics: dict[str, Any],
    evidence_points: list[str],
    pain_signals: list[str],
) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    primary_url = prospect.get("project_url") or prospect.get("email_source_url")
    source_type = "github_repository" if "github.com" in primary_url else "official_website"
    workload_claim_type = "ai_workload" if CAMPAIGN.get("campaignId") == "jungle-grid" else "target_workload"
    for point in evidence_points:
        item = build_evidence(prospect, workload_claim_type, point, primary_url, source_type)
        if item:
            evidence.append(item)
    for signal in pain_signals:
        item = build_evidence(prospect, "infrastructure_pain", signal, primary_url, source_type)
        if item:
            evidence.append(item)
    if prospect.get("updated_at") or prospect.get("pushed_at"):
        activity = f"Repository activity timestamp: {prospect.get('updated_at') or prospect.get('pushed_at')}"
        item = build_evidence(prospect, "activity", activity, primary_url, source_type, directness="direct")
        if item:
            evidence.append(item)
    primary_contact = next(iter(prospect.get("contact_points", [])), {})
    contact_value = prospect.get("email") or primary_contact.get("value", "")
    contact_source_url = (
        prospect.get("email_source_url") or primary_contact.get("source_url", "")
    )
    contact_source_type = (
        prospect.get("email_source_type") or primary_contact.get("type", "official_website")
    )
    contact_claim = f"{contact_value} is publicly listed at {contact_source_url}"
    contact_item = build_evidence(
        prospect,
        "contact",
        contact_claim,
        contact_source_url,
        contact_source_type,
        directness="direct",
    )
    if contact_item:
        evidence.append(contact_item)
    relationship = "verified maintainer relationship" if prospect.get("owner_type", "").lower() == "user" else "project contact relationship"
    role_item = build_evidence(
        prospect,
        "role",
        f"{prospect['name']} has a {relationship} for {prospect['project']}",
        prospect.get("project_url") or contact_source_url,
        source_type,
        directness="strong_inference" if prospect.get("owner_type", "").lower() != "user" else "direct",
    )
    if role_item:
        evidence.append(role_item)
    if diagnostics.get("has_execution_surface"):
        surface_item = build_evidence(
            prospect,
            "integration_surface",
            f"{prospect['project']} exposes a campaign-relevant execution or integration surface",
            primary_url,
            source_type,
        )
        if surface_item:
            evidence.append(surface_item)
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for item in evidence:
        if item["evidence_id"] in seen:
            continue
        seen.add(item["evidence_id"])
        unique.append(item)
    return unique


def request_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 45,
) -> Any:
    body = json.dumps(payload).encode() if payload is not None else None
    request_headers = {"Accept": "application/json", **(headers or {})}
    if payload is not None:
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        url,
        data=body,
        headers=request_headers,
        method=method,
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def source_email(text: str, source_url: str) -> str | None:
    for match in EMAIL_RE.finditer(text):
        email = match.group(0).lower().rstrip(".,;:")
        local, _, domain = email.partition("@")
        if not source_url or not domain:
            continue
        if local in {"example", "test"} or local in GENERIC_EMAIL_LOCAL_PARTS:
            continue
        if domain in {"example.com", "example.org", "users.noreply.github.com"}:
            continue
        context = text[max(0, match.start() - 100) : match.end() + 140]
        if CONTACT_CONTEXT_RE.search(context):
            return email
    return None


def profile_email_source(email: str | None, source_url: str) -> str | None:
    if not email:
        return None
    normalized = email.strip().lower().rstrip(".,;:")
    local, _, domain = normalized.partition("@")
    if not domain:
        return None
    if local in {"donotreply", "do-not-reply", "example", "test"} or local in GENERIC_EMAIL_LOCAL_PARTS:
        return None
    if domain in {"example.com", "example.org", "example.net", "users.noreply.github.com"}:
        return None
    if not EMAIL_RE.fullmatch(normalized):
        return None
    return normalized


def contact_source_score(source_type: str, email: str, context: str) -> int:
    return (contact_quality(email, source_type, context) * 10) + (8 if CONTACT_CONTEXT_RE.search(context) else 0)


def pick_best_contact(candidates: list[dict[str, str]]) -> dict[str, str] | None:
    if not candidates:
        return None
    unique: dict[tuple[str, str], dict[str, str]] = {}
    for candidate in candidates:
        key = (candidate["email"], candidate["source_url"])
        previous = unique.get(key)
        if not previous or contact_source_score(
            candidate["source_type"], candidate["email"], candidate.get("context", "")
        ) > contact_source_score(previous["source_type"], previous["email"], previous.get("context", "")):
            unique[key] = candidate
    return max(
        unique.values(),
        key=lambda candidate: contact_source_score(
            candidate["source_type"], candidate["email"], candidate.get("context", "")
        ),
    )


def category_for(text: str) -> str:
    lowered = text.lower()
    if "model context protocol" in lowered or re.search(r"\bmcp\b", lowered):
        return "mcp"
    if re.search(r"\b(vllm|ollama|runpod|modal|replicate|serverless gpu|gpu orchestration|model serving)\b", lowered):
        return "ai_infrastructure"
    if re.search(r"\b(fine[- ]?tun|training|inference|rag|evals?|evaluation)\b", lowered):
        return "inference_training"
    if disambiguates_ai_agent(lowered) and re.search(r"\b(runtime|worker|queue|background|compute|long[- ]running)\b", lowered):
        return "agent_compute"
    if re.search(r"\b(workflow|automation|scheduler)\b", lowered) and re.search(
        r"\b(llm|agent|mcp|inference|rag|evals?|worker|queue)\b", lowered
    ):
        return "workflow_automation"
    if disambiguates_ai_agent(lowered):
        return "agent_framework"
    if re.search(r"\b(llm|rag|evals?|prompt|model)\b", lowered):
        return "llm_application"
    return "open_source_ai"


def normalize_prospect(raw: dict[str, Any]) -> dict[str, Any] | None:
    email = str(raw.get("email", "")).strip().lower()
    raw_contacts = raw.get("contact_points")
    contact_points = raw_contacts if isinstance(raw_contacts, list) else []
    primary_contact = next(
        (item for item in contact_points if isinstance(item, dict) and item.get("value")),
        {},
    )
    source_url = str(
        raw.get("email_source_url") or primary_contact.get("source_url") or ""
    ).strip()
    project = str(raw.get("project", "")).strip()
    project_url = str(raw.get("project_url", "")).strip()
    if not source_url or not project or not project_url:
        return None
    if email and not EMAIL_RE.fullmatch(email):
        return None
    owner = str(raw.get("owner_login") or project.split("/", 1)[0]).strip()
    cleaned_research = clean_research_text(str(raw.get("research_text") or raw.get("project_description") or "").strip())
    if not contact_points and email:
        contact_points = [
            {
                "type": "email",
                "value": email,
                "source_url": source_url,
                "publicly_listed": True,
                "authorized": True,
                "confidence": contact_quality(
                    email,
                    str(raw.get("email_source_type") or "official_website"),
                    cleaned_research,
                )
                / 10,
            }
        ]
    return {
        "prospect_id": str(raw.get("prospect_id") or uuid.uuid4()),
        "schema_version": "2.0",
        "entity_id": str(raw.get("entity_id") or f"project:{normalize_name(project).replace(' ', '-')}"),
        "name": str(raw.get("name") or project.split("/")[-1]).strip(),
        "email": email,
        "email_source_url": source_url,
        "email_source_type": str(raw.get("email_source_type") or "official_website"),
        "contact_points": contact_points,
        "contact_provenance": raw.get("contact_provenance")
        or {
            "value": email or str(primary_contact.get("value") or ""),
            "source_url": source_url,
            "source_type": str(
                raw.get("email_source_type")
                or primary_contact.get("type")
                or "official_website"
            ),
            "publicly_listed": bool(primary_contact.get("publicly_listed", True)),
            "person_project_match": "verified" if str(raw.get("owner_type") or "").lower() == "user" else "project_contact",
            "verification_method": "public_source",
            "confidence": (
                contact_quality(
                    email,
                    str(raw.get("email_source_type") or "official_website"),
                    cleaned_research,
                )
                / 10
                if email
                else float(primary_contact.get("confidence") or 0)
            ),
            "collected_at": utc_now(),
            "appropriate_use_category": "professional_outreach",
        },
        "project": project,
        "project_key": normalize_name(project).replace(" ", "-"),
        "project_url": project_url,
        "project_description": str(raw.get("project_description") or "").strip(),
        "category": str(raw.get("category") or category_for(f"{project} {cleaned_research}")),
        "research_text": cleaned_research,
        "evidence_urls": unique_preserve_order(
            [
                source_url,
                project_url,
                *[str(url) for url in raw.get("evidence_urls", []) if url],
            ]
        ),
        "stars": int(raw.get("stars") or 0),
        "active": bool(raw.get("active", True)),
        "owner_login": owner,
        "owner_type": str(raw.get("owner_type") or ""),
        "updated_at": str(raw.get("updated_at") or raw.get("pushed_at") or ""),
        "pushed_at": str(raw.get("pushed_at") or raw.get("updated_at") or ""),
        "open_issues_count": int(raw.get("open_issues_count") or 0),
        "readme_present": bool(cleaned_research),
    }


def load_json_env_list(name: str) -> set[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return set()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        LOG.warning("Ignoring invalid JSON in %s.", name)
        return set()
    if not isinstance(parsed, list):
        LOG.warning("Ignoring non-list value in %s.", name)
        return set()
    return {str(value).strip().lower() for value in parsed if str(value).strip()}


def load_seed(input_path: Path | None) -> list[dict[str, Any]]:
    if not input_path or not input_path.exists():
        return []
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    rows = payload if isinstance(payload, list) else payload.get("prospects", [])
    return [prospect for row in rows if (prospect := normalize_prospect(row))]


def github_headers() -> dict[str, str]:
    headers = {
        "User-Agent": "openline/0.1",
        "Accept": "application/vnd.github+json",
    }
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def fetch_text(url: str, timeout: int = 20, limit: int = 120_000) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "openline/0.1"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read(limit).decode("utf-8", errors="replace")


def is_docs_url(url: urllib.parse.ParseResult, base: urllib.parse.ParseResult) -> bool:
    hostname = (url.hostname or "").lower()
    base_hostname = (base.hostname or "").lower().removeprefix("www.")
    return bool(
        LIKELY_DOCS_PATH_RE.search(url.path or "/")
        or hostname.startswith("docs.")
        or hostname == f"docs.{base_hostname}"
        or hostname.endswith("readthedocs.io")
        or hostname.endswith("github.io")
        or hostname.endswith("mintlify.app")
    )


def extract_likely_site_links(html: str, base_urls: list[str]) -> list[str]:
    parsed_bases: list[urllib.parse.ParseResult] = []
    for base_url in base_urls:
        try:
            parsed_bases.append(urllib.parse.urlparse(base_url))
        except ValueError:
            continue
    if not parsed_bases:
        return []
    links: list[str] = []
    for match in HREF_RE.finditer(html):
        raw = (match.group(1) or "").strip()
        if not raw:
            continue
        try:
            absolute = urllib.parse.urljoin(base_urls[0], raw)
            parsed = urllib.parse.urlparse(absolute)
        except ValueError:
            continue
        if parsed.scheme not in {"http", "https"}:
            continue
        if not any(parsed.netloc == base.netloc or is_docs_url(parsed, base) for base in parsed_bases):
            continue
        if not LIKELY_CONTACT_PATH_RE.search(parsed.path or "/") and not any(
            is_docs_url(parsed, base) for base in parsed_bases
        ):
            continue
        links.append(parsed.geturl())
    return list(dict.fromkeys(links))


def public_contact_points(html: str, source_url: str) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    links = [match.group(1).strip() for match in HREF_RE.finditer(html)]
    links.extend(match.group(1).strip() for match in FORM_ACTION_RE.finditer(html))
    if "<form" in html.lower() and CONTACT_CONTEXT_RE.search(html):
        links.append(source_url)
    for raw in links:
        absolute = urllib.parse.urljoin(source_url, raw)
        lowered = absolute.lower()
        point_type = ""
        if raw.lower().startswith("mailto:"):
            continue
        if raw.lower().startswith("tel:"):
            point_type = "business_phone"
            absolute = raw.split(":", 1)[1]
        elif "wa.me/" in lowered or "whatsapp" in lowered:
            point_type = "whatsapp_business"
        elif "github.com" in lowered and "/discussions" in lowered:
            point_type = "github_discussions"
        elif "github.com" in lowered and "/issues" in lowered:
            point_type = "github_issue"
        elif "github.com" in lowered:
            point_type = "github_profile"
        elif "linkedin.com/company/" in lowered:
            point_type = "linkedin_company"
        elif "linkedin.com/in/" in lowered:
            point_type = "linkedin_profile"
        elif "discord.gg/" in lowered or "discord.com/invite/" in lowered:
            point_type = "discord"
        elif "slack.com" in lowered:
            point_type = "slack"
        elif "x.com/" in lowered or "twitter.com/" in lowered:
            point_type = "x"
        elif "facebook.com/" in lowered:
            point_type = "facebook_page"
        elif "instagram.com/" in lowered:
            point_type = "instagram_business"
        elif "calendly.com/" in lowered or "cal.com/" in lowered or "booking" in lowered:
            point_type = "booking_link"
        elif re.search(r"(?:^|/)(integration|integrations)(?:/|$)", lowered):
            point_type = "integration_form"
        elif re.search(r"(?:^|/)(partner|partners|partnerships)(?:/|$)", lowered):
            point_type = "partnership_form"
        elif re.search(r"(?:^|/)(marketplace|submit)(?:/|$)", lowered):
            point_type = "marketplace_form"
        elif re.search(r"(?:^|/)(feature-requests?|ideas|roadmap)(?:/|$)", lowered):
            point_type = "feature_request_portal"
        elif re.search(r"(?:^|/)(community|forum|discuss)(?:/|$)", lowered):
            point_type = "community_forum"
        elif re.search(r"(?:^|/)(contact|support|inquiry|inquiries)(?:/|$)", lowered):
            point_type = "official_contact_form"
        if not point_type:
            continue
        points.append(
            {
                "type": point_type,
                "value": absolute,
                "source_url": source_url,
                "publicly_listed": True,
                "authorized": point_type not in {"github_issue", "discord", "slack"},
                "confidence": 0.9 if point_type in {
                    "official_contact_form",
                    "partnership_form",
                    "integration_form",
                    "booking_link",
                } else 0.75,
            }
        )
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for point in points:
        unique[(point["type"], point["value"])] = point
    return list(unique.values())


def website_contacts(
    start_urls: list[str] | str,
    source_type: str = "official_website",
    include_contact_points: bool = False,
) -> (
    tuple[list[dict[str, str]], str]
    | tuple[list[dict[str, str]], str, list[dict[str, Any]]]
):
    queue = [start_urls] if isinstance(start_urls, str) else [url for url in start_urls if url]
    queue = list(dict.fromkeys(queue))
    seed_urls = list(queue)
    parsed_bases: list[urllib.parse.ParseResult] = []
    for url in queue:
        try:
            parsed = urllib.parse.urlparse(url)
        except ValueError:
            continue
        if parsed.scheme in {"http", "https"}:
            parsed_bases.append(parsed)
    if not parsed_bases:
        return ([], "", []) if include_contact_points else ([], "")
    visited: set[str] = set()
    latest_text = ""
    contacts: list[dict[str, str]] = []
    contact_points: list[dict[str, Any]] = []
    while queue and len(visited) < 5:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        try:
            text = fetch_text(current)
        except (urllib.error.URLError, TimeoutError, ValueError, socket.timeout, http.client.HTTPException):
            continue
        latest_text = text
        contact_points.extend(public_contact_points(text, current))
        parsed_current = urllib.parse.urlparse(current)
        effective_source_type = (
            "project_docs" if any(is_docs_url(parsed_current, base) for base in parsed_bases) else source_type
        )
        for email in extract_public_emails(text, current):
            contacts.append(
                {
                    "email": email,
                    "source_url": current,
                    "source_type": effective_source_type,
                    "context": text[:5000],
                }
            )
        for link in extract_likely_site_links(text, seed_urls):
            if link not in visited and link not in queue:
                queue.append(link)
    unique_points: dict[tuple[str, str], dict[str, Any]] = {}
    for point in contact_points:
        unique_points[(point["type"], point["value"])] = point
    if include_contact_points:
        return contacts, latest_text, list(unique_points.values())
    return contacts, latest_text


def extract_public_emails(text: str, source_url: str) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for match in EMAIL_RE.finditer(text):
        email = match.group(0).lower().rstrip(".,;:")
        if email in seen:
            continue
        local, _, domain = email.partition("@")
        if not source_url or not domain:
            continue
        if local in {"example", "test"} or local in GENERIC_EMAIL_LOCAL_PARTS:
            continue
        if domain in {"example.com", "example.org", "users.noreply.github.com"}:
            continue
        context = text[max(0, match.start() - 100) : match.end() + 140]
        if CONTACT_CONTEXT_RE.search(context):
            seen.add(email)
            found.append(email)
    return found


def fetch_registry_json(url: str) -> Any:
    try:
        return request_json(url, headers={"User-Agent": "openline/0.1"}, timeout=20)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return None


def get_raw_repo_file(owner: str, repo: str, branch: str, path: str) -> str | None:
    url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
    try:
        return fetch_text(url)
    except (urllib.error.URLError, TimeoutError, ValueError):
        return None


def package_registry_metadata(owner: str, repo: str, branch: str) -> dict[str, list[str]]:
    project_urls: set[str] = set()
    package_pages: set[str] = set()
    package_json = get_raw_repo_file(owner, repo, branch, "package.json")
    if package_json:
        try:
            parsed = json.loads(package_json)
            name = str(parsed.get("name") or "").strip()
            if name:
                package_pages.add(f"https://www.npmjs.com/package/{urllib.parse.quote(name)}")
                npm = fetch_registry_json(f"https://registry.npmjs.org/{urllib.parse.quote(name)}") or {}
                latest_tag = ((npm.get("dist-tags") or {}).get("latest")) if isinstance(npm, dict) else None
                latest = (((npm.get("versions") or {}).get(latest_tag)) if latest_tag else None) if isinstance(npm, dict) else None
                if isinstance(latest, dict):
                    for value in [latest.get("homepage"), (latest.get("bugs") or {}).get("url")]:
                        if isinstance(value, str) and value.strip():
                            project_urls.add(value.strip())
                homepage = parsed.get("homepage")
                if isinstance(homepage, str) and homepage.strip():
                    project_urls.add(homepage.strip())
        except (ValueError, TypeError, AttributeError):
            pass

    pyproject = get_raw_repo_file(owner, repo, branch, "pyproject.toml")
    if pyproject:
        match = re.search(r'^\s*name\s*=\s*["\']([^"\']+)["\']', pyproject, re.M)
        if match:
            package_pages.add(f"https://pypi.org/project/{urllib.parse.quote(match.group(1))}/")
            pypi = fetch_registry_json(f"https://pypi.org/pypi/{urllib.parse.quote(match.group(1))}/json") or {}
            info = pypi.get("info") if isinstance(pypi, dict) else {}
            if isinstance(info, dict):
                for value in [info.get("home_page"), *(info.get("project_urls") or {}).values()]:
                    if isinstance(value, str) and value.strip():
                        project_urls.add(value.strip())

    cargo = get_raw_repo_file(owner, repo, branch, "Cargo.toml")
    if cargo:
        match = re.search(r'^\s*name\s*=\s*["\']([^"\']+)["\']', cargo, re.M)
        if match:
            package_pages.add(f"https://crates.io/crates/{urllib.parse.quote(match.group(1))}")
            crates = fetch_registry_json(f"https://crates.io/api/v1/crates/{urllib.parse.quote(match.group(1))}") or {}
            crate = crates.get("crate") if isinstance(crates, dict) else {}
            if isinstance(crate, dict):
                for value in [crate.get("homepage"), crate.get("documentation"), crate.get("repository")]:
                    if isinstance(value, str) and value.strip():
                        project_urls.add(value.strip())

    return {"project_urls": list(project_urls), "package_pages": list(package_pages)}


def package_registry_contacts(package_pages: list[str]) -> list[dict[str, str]]:
    contacts: list[dict[str, str]] = []
    for url in package_pages:
        try:
            text = fetch_text(url)
        except (urllib.error.URLError, TimeoutError, ValueError):
            continue
        for email in extract_public_emails(text, url):
            contacts.append(
                {
                    "email": email,
                    "source_url": url,
                    "source_type": "package_page",
                    "context": text[:5000],
                }
            )
    return contacts


QUERY_FAMILIES: dict[str, dict[str, Any]] = {
    "mcp_servers": {
        "categories": {"mcp", "agent_framework", "agent_compute"},
        "github": [
            '"MCP server" agent workflow stars:>4',
            '"model context protocol" tools stars:>4',
            '"OpenAI agents" worker stars:>4',
        ],
        "registry": ["mcp server", "model context protocol tools", "openai agents worker"],
    },
    "agent_jobs": {
        "categories": {"agent_compute", "agent_framework", "workflow_automation"},
        "github": [
            '"AI agent" "background jobs" stars:>4',
            '"agent runtime" queue stars:>4',
            "CrewAI production stars:>4",
            "LangGraph queue stars:>4",
        ],
        "registry": ["ai agent background jobs", "agent runtime queue", "crewai production", "langgraph queue"],
    },
    "eval_and_rag": {
        "categories": {"inference_training", "workflow_automation", "llm_application"},
        "github": [
            '"LLM eval" batch stars:>4',
            '"RAG pipeline" worker stars:>4',
            '"workflow automation" llm workers stars:>4',
        ],
        "registry": ["llm eval batch", "rag pipeline worker", "workflow automation llm workers"],
    },
    "model_ops": {
        "categories": {"inference_training", "ai_infrastructure"},
        "github": [
            '"fine-tuning" GPU stars:>4',
            '"inference server" deployment stars:>4',
            '"vLLM" deployment stars:>4',
            '"Qwen" inference stars:>4',
            '"Ollama" server stars:>4',
        ],
        "registry": ["fine tuning gpu", "inference server deployment", "vllm deployment", "qwen inference", "ollama server"],
    },
    "gpu_platform_alt": {
        "categories": {"ai_infrastructure", "workflow_automation"},
        "github": [
            '"RunPod" GPU stars:>4',
            '"Modal" batch stars:>4',
            '"Replicate alternative" stars:>4',
            '"serverless GPU" stars:>4',
            '"AI workflow scheduler" stars:>4',
        ],
        "registry": ["runpod gpu", "modal batch", "replicate alternative", "serverless gpu", "ai workflow scheduler"],
    },
    "tooling": {
        "categories": {"workflow_automation", "llm_application", "agent_framework"},
        "github": [
            '"Dify" "custom tool" stars:>4',
            '"Flowise" deployment stars:>4',
            '"agent runtime" stars:>4',
        ],
        "registry": ["dify custom tool", "flowise deployment", "agent runtime"],
    },
}


def query_seed() -> int:
    raw = os.getenv("OUTREACH_QUERY_SEED", "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError:
            return sum(ord(char) for char in raw)
    return int(datetime.now(timezone.utc).strftime("%Y%m%d"))


def query_pack_order(category: str | None) -> list[dict[str, Any]]:
    if CAMPAIGN.get("campaignId") != "jungle-grid":
        icp = CAMPAIGN["idealCustomerProfile"]
        target_terms = [str(term) for term in icp.get("targetTerms", [])[:4]]
        workload_terms = [str(term) for term in icp.get("workloadTerms", [])[:4]]
        execution_terms = [str(term) for term in icp.get("executionTerms", [])[:3]]
        github_queries = [
            f'"{target}" "{workload}" stars:>4'
            for target, workload in zip(target_terms, workload_terms, strict=False)
        ]
        github_queries.extend(
            f'"{target_terms[0]}" "{term}" stars:>4'
            for term in execution_terms
            if target_terms
        )
        registry_queries = [
            f"{target} {workload}"
            for target, workload in zip(target_terms, workload_terms, strict=False)
        ]
        return [
            {
                "categories": set(icp.get("categories", [])),
                "github": unique_preserve_order(github_queries),
                "registry": unique_preserve_order(registry_queries),
            }
        ]
    packs = [
        pack
        for pack in QUERY_FAMILIES.values()
        if category is None or not pack.get("categories") or category in pack["categories"]
    ]
    random.Random(query_seed()).shuffle(packs)
    return packs


def parse_github_repo(value: str | None) -> tuple[str, str] | None:
    if not value:
        return None
    normalized = value.strip().removeprefix("git+")
    normalized = re.sub(r"\.git$", "", normalized, flags=re.I)
    match = re.search(r"github\.com/([^/]+)/([^/#?]+)", normalized, re.I)
    if not match:
        return None
    return match.group(1), match.group(2)


def account_search_query(query: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"\bstars:\>[0-9]+\b| in:[^ ]+", "", query)).strip()


def search_registry_projects(term: str) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    npm = fetch_registry_json(f"https://registry.npmjs.org/-/v1/search?text={urllib.parse.quote(term)}&size=8") or {}
    for item in npm.get("objects", []) if isinstance(npm, dict) else []:
        links = ((item.get("package") or {}).get("links") or {}) if isinstance(item, dict) else {}
        if isinstance(links, dict):
            for value in links.values():
                repo = parse_github_repo(value if isinstance(value, str) else None)
                if repo:
                    candidates.append(repo)
    crates = fetch_registry_json(f"https://crates.io/api/v1/crates?page=1&per_page=8&q={urllib.parse.quote(term)}") or {}
    for item in crates.get("crates", []) if isinstance(crates, dict) else []:
        if not isinstance(item, dict):
            continue
        for value in [item.get("repository"), item.get("homepage"), item.get("documentation")]:
            repo = parse_github_repo(value if isinstance(value, str) else None)
            if repo:
                candidates.append(repo)
    return list(dict.fromkeys(candidates))


def github_profile(login: str) -> dict[str, Any] | None:
    try:
        return request_json(f"https://api.github.com/users/{urllib.parse.quote(login)}", headers=github_headers())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return None


def github_readme(full_name: str, default_branch: str) -> str:
    readme_url = f"https://raw.githubusercontent.com/{full_name}/{default_branch}/README.md"
    try:
        return fetch_text(readme_url, limit=100_000)
    except (urllib.error.URLError, TimeoutError, ValueError, socket.timeout, http.client.HTTPException):
        return ""


def repo_to_prospect(repo: dict[str, Any], category: str | None) -> dict[str, Any] | None:
    full_name = str(repo.get("full_name") or "").strip()
    owner_login = str((repo.get("owner") or {}).get("login") or "").strip()
    if not full_name or not owner_login:
        return None
    default_branch = str(repo.get("default_branch") or "main")
    profile = github_profile(owner_login) or {}
    readme = github_readme(full_name, default_branch)
    metadata = package_registry_metadata(owner_login, full_name.split("/", 1)[1], default_branch)

    contacts: list[dict[str, str]] = []
    profile_contact = profile_email_source(profile.get("email"), profile.get("html_url") or f"https://github.com/{owner_login}")
    if profile_contact:
        contacts.append(
            {
                "email": profile_contact,
                "source_url": profile.get("html_url") or f"https://github.com/{owner_login}",
                "source_type": "github_profile",
                "context": "Public email field on the professional GitHub profile.",
            }
        )
    if readme:
        for email in extract_public_emails(readme, f"https://github.com/{full_name}#readme"):
            contacts.append(
                {
                    "email": email,
                    "source_url": f"https://github.com/{full_name}#readme",
                    "source_type": "repository_readme",
                    "context": readme[:5000],
                }
            )
    website_seeds = [
        str(repo.get("homepage") or "").strip(),
        str(profile.get("blog") or "").strip(),
        *metadata["project_urls"],
    ]
    homepage_contacts, homepage_text, homepage_points = website_contacts(
        website_seeds, include_contact_points=True
    )
    contacts.extend(homepage_contacts)
    contacts.extend(package_registry_contacts(metadata["package_pages"]))
    best_contact = pick_best_contact(contacts)
    github_points = [
        {
            "type": "github_profile",
            "value": profile.get("html_url") or f"https://github.com/{owner_login}",
            "source_url": profile.get("html_url") or f"https://github.com/{owner_login}",
            "publicly_listed": True,
            "authorized": True,
            "confidence": 0.9,
        },
        {
            "type": "github_discussions",
            "value": f"https://github.com/{full_name}/discussions",
            "source_url": repo.get("html_url") or f"https://github.com/{full_name}",
            "publicly_listed": True,
            "authorized": True,
            "confidence": 0.75,
        },
    ]
    contact_points = [*homepage_points, *github_points]
    if best_contact:
        contact_points.insert(
            0,
            {
                "type": "email",
                "value": best_contact["email"],
                "source_url": best_contact["source_url"],
                "publicly_listed": True,
                "authorized": True,
                "confidence": contact_quality(
                    best_contact["email"],
                    best_contact["source_type"],
                    best_contact.get("context", ""),
                )
                / 10,
            },
        )
    if not contact_points:
        return None

    combined_text = clean_research_text(f"{repo.get('description', '')} {readme or homepage_text}")

    return normalize_prospect(
        {
            "name": profile.get("name") or owner_login,
            "email": best_contact["email"] if best_contact else "",
            "email_source_url": (
                best_contact["source_url"] if best_contact else contact_points[0]["source_url"]
            ),
            "email_source_type": (
                best_contact["source_type"] if best_contact else "official_website"
            ),
            "contact_points": contact_points,
            "project": full_name,
            "project_url": repo.get("html_url") or f"https://github.com/{full_name}",
            "project_description": repo.get("description") or "",
            "category": category or category_for(combined_text[:4000]),
            "research_text": combined_text[:20_000],
            "evidence_urls": [
                best_contact["source_url"] if best_contact else contact_points[0]["source_url"],
                repo.get("html_url") or f"https://github.com/{full_name}",
                f"https://github.com/{full_name}#readme",
                *metadata["project_urls"],
            ],
            "stars": repo.get("stargazers_count") or 0,
            "active": True,
            "owner_login": owner_login,
            "owner_type": str((repo.get("owner") or {}).get("type") or ""),
            "updated_at": str(repo.get("updated_at") or ""),
            "pushed_at": str(repo.get("pushed_at") or ""),
            "open_issues_count": int(repo.get("open_issues_count") or 0),
        }
    )


def discover_from_profiles(
    queries: list[str], category: str | None, target: int, seen_projects: set[str], seen_profiles: set[str]
) -> list[dict[str, Any]]:
    prospects: list[dict[str, Any]] = []
    for profile_type in ("user", "org"):
        for query in queries:
            if len(prospects) >= target:
                return prospects
            url = "https://api.github.com/search/users?" + urllib.parse.urlencode(
                {"q": f"{account_search_query(query)} in:login,fullname,bio type:{profile_type}", "per_page": 10, "page": 1}
            )
            try:
                result = request_json(url, headers=github_headers())
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
                return prospects
            for user in result.get("items", []):
                login = str(user.get("login") or "").strip()
                if not login or login.lower() in seen_profiles:
                    continue
                seen_profiles.add(login.lower())
                repos_url = (
                    f"https://api.github.com/orgs/{urllib.parse.quote(login)}/repos"
                    if profile_type == "org"
                    else f"https://api.github.com/users/{urllib.parse.quote(login)}/repos"
                )
                try:
                    repos = request_json(
                        repos_url + "?" + urllib.parse.urlencode({"sort": "updated", "per_page": 10}),
                        headers=github_headers(),
                    )
                except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
                    continue
                for repo in repos if isinstance(repos, list) else []:
                    full_name = str(repo.get("full_name") or "").strip().lower()
                    if not full_name or repo.get("fork") or repo.get("archived") or full_name in seen_projects:
                        continue
                    seen_projects.add(full_name)
                    prospect = repo_to_prospect(repo, category)
                    if prospect:
                        prospects.append(prospect)
                        break
    return prospects


def load_memory() -> dict[str, set[str]]:
    path = dedupe_store_path()
    if not path.exists():
        return {"emails": set(), "owners": set(), "repos": set(), "domains": set(), "names": set()}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"emails": set(), "owners": set(), "repos": set(), "domains": set(), "names": set()}
    return {
        "emails": {str(value).strip().lower() for value in payload.get("emails", [])},
        "owners": {str(value).strip().lower() for value in payload.get("owners", [])},
        "repos": {str(value).strip().lower() for value in payload.get("repos", [])},
        "domains": {str(value).strip().lower() for value in payload.get("domains", [])},
        "names": {str(value).strip().lower() for value in payload.get("names", [])},
    }


def persist_memory(prospects: list[dict[str, Any]]) -> None:
    path = dedupe_store_path()
    memory = load_memory()
    for prospect in prospects:
        email = prospect.get("email", "").lower()
        domain = email.partition("@")[2]
        if email:
            memory["emails"].add(email)
        memory["owners"].add(owner_key(prospect.get("owner_login", "")))
        memory["repos"].add(prospect["project_url"].strip().lower())
        if domain:
            memory["domains"].add(domain)
        memory["names"].add(normalize_name(prospect["name"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({key: sorted(values) for key, values in memory.items()}, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def qualification_diagnostics(prospect: dict[str, Any]) -> dict[str, Any]:
    text = clean_research_text(
        " ".join(
            [
                prospect.get("research_text", ""),
                prospect.get("project_description", ""),
            ]
        )
    )
    email = prospect.get("email", "").lower()
    local, _, domain = email.partition("@")
    owner = prospect.get("owner_login", "")
    updated_days = days_since(prospect.get("updated_at") or prospect.get("pushed_at"))
    evidence_points = extract_evidence_points(text)
    pain_signals = extract_pain_signals(text)
    repo_label = f"{prospect.get('project', '')} {prospect.get('project_description', '')}"
    target_re = campaign_regex("targetTerms")
    workload_re = campaign_regex("workloadTerms")
    execution_re = campaign_regex("executionTerms")
    exclusion_re = campaign_regex("exclusionTerms")
    has_target_signal = bool(target_re.search(text)) and bool(evidence_points)
    has_workload_signal = bool(workload_re.search(text)) and bool(evidence_points)
    has_execution_surface = bool(execution_re.search(text)) and bool(evidence_points)
    has_pain_signal = bool(pain_signals)
    generic_package = bool(GENERIC_PACKAGE_RE.search(text)) and not has_workload_signal
    configured_exclusion = bool(exclusion_re.search(text))
    qualification = CAMPAIGN["qualification"]
    maximum_age = int(qualification.get("maximumActivityAgeDays", 180))
    contamination = contamination_reasons(str(prospect.get("research_text", "")))
    excluded_rule: str | None = None
    missing_evidence: list[str] = []

    if contamination:
        excluded_rule = "contaminated_evidence"
    elif generic_package:
        excluded_rule = "generic_package_without_ai_workload"
    elif configured_exclusion:
        excluded_rule = "campaign_exclusion_term"
    elif is_large_org(owner):
        excluded_rule = "large_vendor_or_foundation_org"
    elif email and is_generic_contact_email(email):
        excluded_rule = "generic_contact_email"
    elif REPO_TYPE_EXCLUSION_RE.search(repo_label):
        excluded_rule = "repo_type_excluded"
    elif not prospect.get("readme_present", False):
        excluded_rule = "missing_meaningful_readme"
    elif NON_AI_AGENT_RE.search(text):
        excluded_rule = "non_ai_agent_context"
    elif not evidence_points:
        excluded_rule = "no_concrete_ai_workload_evidence"
    elif qualification.get("requireTargetSignal", True) and not has_target_signal:
        excluded_rule = "missing_campaign_target_signal"
    elif qualification.get("requireWorkloadSignal", True) and not has_workload_signal:
        excluded_rule = "missing_campaign_workload_signal"
    elif qualification.get("requireExecutionSignal", False) and not has_execution_surface:
        excluded_rule = "missing_campaign_execution_signal"
    elif qualification.get("requirePainSignal", False) and not has_pain_signal:
        excluded_rule = "missing_campaign_pain_signal"
    elif updated_days is None or updated_days > maximum_age:
        excluded_rule = "stale_project"
    elif not prospect.get("contact_points"):
        excluded_rule = "missing_public_contact_point"

    if not evidence_points:
        missing_evidence.append("concrete workload execution evidence")
    if qualification.get("requireTargetSignal", True) and not has_target_signal:
        missing_evidence.append("campaign target signal")
    if qualification.get("requireWorkloadSignal", True) and not has_workload_signal:
        missing_evidence.append("campaign workload signal")
    if qualification.get("requireExecutionSignal", False) and not has_execution_surface:
        missing_evidence.append("campaign execution or integration signal")
    if contamination:
        missing_evidence.append("clean uncontaminated evidence")
    if len(pain_signals) < 1:
        missing_evidence.append("execution pain signal")
    if updated_days is None or updated_days > maximum_age:
        missing_evidence.append("recent activity")
    contact_score = (
        contact_quality(email, prospect["email_source_type"], text[:2000])
        if email
        else round(
            max(
                (
                    float(contact.get("confidence", 0))
                    for contact in prospect.get("contact_points", [])
                    if isinstance(contact, dict)
                ),
                default=0,
            )
            * 10
        )
    )
    if contact_score < 5:
        missing_evidence.append("verified public contact point")
        if excluded_rule is None:
            excluded_rule = "low_quality_contact"
    if not re.search(r"\b(founder|maintainer|solo|indie|small team|developer)\b", text, re.I) and prospect.get(
        "owner_type", ""
    ).lower() != "user":
        missing_evidence.append("small-team or maintainer context")

    evidence_strength = min(
        1.0,
        0.2
        + (0.2 * min(len(evidence_points), 3))
        + (0.15 * min(len(pain_signals), 2))
        + (0.15 if (updated_days is not None and updated_days <= 45) else 0)
        + (0.1 if prospect.get("owner_type", "").lower() == "user" else 0)
        + (0.1 if contact_score >= 7 else 0),
    )
    return {
        "excluded": excluded_rule is not None,
        "skip_reason": excluded_rule or ("missing_required_evidence" if missing_evidence else ""),
        "exclusion_rule_triggered": excluded_rule or "",
        "missing_evidence": missing_evidence,
        "duplicate": False,
        "stale": bool(updated_days is None or updated_days > maximum_age),
        "generic": bool(email and is_generic_contact_email(email)),
        "irrelevant": not evidence_points or not has_target_signal,
        "generic_package": generic_package,
        "contamination_reasons": contamination,
        "has_direct_ai_workload": has_workload_signal,
        "has_campaign_target_signal": has_target_signal,
        "has_campaign_workload_signal": has_workload_signal,
        "has_execution_surface": has_execution_surface,
        "large_company": is_large_org(owner),
        "owner_key": owner_key(owner),
        "contact_quality": contact_score,
        "generic_team_email": bool(email and is_team_email(email)),
        "updated_days": updated_days,
        "evidence_points": evidence_points,
        "pain_signals": pain_signals,
        "small_team_context": bool(
            prospect.get("owner_type", "").lower() == "user"
            or re.search(r"\b(founder|maintainer|solo|indie|small team)\b", text, re.I)
        ),
        "readme_quality": "meaningful" if prospect.get("readme_present", False) and len(text) >= 140 else "thin",
    }


def discover_from_github(target: int, category: str | None) -> list[dict[str, Any]]:
    prospects: list[dict[str, Any]] = []
    seen_projects: set[str] = set()
    seen_profiles: set[str] = set()
    for pack in query_pack_order(category):
        queries = list(pack["github"])
        random.Random(query_seed() + len(prospects)).shuffle(queries)
        for query in queries:
            if len(prospects) >= target * 4:
                break
            for page in (1, 2):
                if len(prospects) >= target * 4:
                    break
                url = "https://api.github.com/search/repositories?" + urllib.parse.urlencode(
                    {"q": f"{query} archived:false fork:false", "sort": "updated", "per_page": 20, "page": page}
                )
                try:
                    result = request_json(url, headers=github_headers())
                except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
                    LOG.warning("GitHub discovery unavailable: %s", error)
                    return prospects
                for repo in result.get("items", []):
                    if len(prospects) >= target * 4:
                        break
                    full_name = str(repo.get("full_name") or "").strip().lower()
                    if not full_name or full_name in seen_projects:
                        continue
                    seen_projects.add(full_name)
                    prospect = repo_to_prospect(repo, category)
                    if prospect:
                        prospects.append(prospect)
            for term in pack["registry"]:
                if len(prospects) >= target * 4:
                    break
                for owner, repo_name in search_registry_projects(term):
                    full_name = f"{owner}/{repo_name}".lower()
                    if full_name in seen_projects:
                        continue
                    try:
                        repo = request_json(
                            f"https://api.github.com/repos/{urllib.parse.quote(owner)}/{urllib.parse.quote(repo_name)}",
                            headers=github_headers(),
                        )
                    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
                        continue
                    seen_projects.add(full_name)
                    prospect = repo_to_prospect(repo, category)
                    if prospect:
                        prospects.append(prospect)
                    if len(prospects) >= target * 4:
                        break
        if len(prospects) < target * 2:
            prospects.extend(
                discover_from_profiles(queries, category, (target * 2) - len(prospects), seen_projects, seen_profiles)
            )
    return [prospect for prospect in prospects if prospect]


def candidate_repository(candidate: SourceCandidate) -> tuple[str, str] | None:
    metadata = candidate.metadata or {}
    values = [
        metadata.get("repository_url"),
        metadata.get("homepage"),
        metadata.get("project_url"),
        candidate.url,
        *[url for url in metadata.get("resolved_urls", []) if isinstance(url, str)],
    ]
    for value in values:
        repo = parse_github_repo(value if isinstance(value, str) else None)
        if repo:
            return repo
    return None


def candidate_official_urls(candidate: SourceCandidate) -> list[str]:
    urls: list[str] = []
    metadata = candidate.metadata or {}
    for value in [
        metadata.get("official_url"),
        metadata.get("homepage"),
        metadata.get("project_url"),
        metadata.get("repository_url"),
        *[url for url in metadata.get("resolved_urls", []) if isinstance(url, str)],
    ]:
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            urls.append(value)
    if candidate.source_type in {"official_website", "gitlab", "huggingface", "docker_hub", "npm", "pypi"}:
        urls.append(candidate.url)
    return list(dict.fromkeys(urls))


def prospect_from_source_candidate(
    adapter: ProspectSourceAdapter,
    candidate: SourceCandidate,
    category: str | None,
    seen_projects: set[str],
) -> dict[str, Any] | None:
    repo = candidate_repository(candidate)
    if repo:
        owner, repo_name = repo
        full_name = f"{owner}/{repo_name}".lower()
        if full_name in seen_projects:
            return None
        try:
            payload = request_json(
                f"https://api.github.com/repos/{urllib.parse.quote(owner)}/{urllib.parse.quote(repo_name)}",
                headers=github_headers(),
            )
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            return None
        seen_projects.add(full_name)
        prospect = repo_to_prospect(payload, category)
        if prospect:
            prospect["discovery_source"] = candidate.source_type
        return prospect

    if candidate.source_type in {"news_rss", "hackernews", "reddit", "stack_exchange", "youtube", "arxiv"}:
        return None

    contacts, page_text, contact_points = website_contacts(
        candidate_official_urls(candidate),
        candidate.source_type,
        include_contact_points=True,
    )
    best_contact = pick_best_contact(contacts)
    if best_contact:
        contact_points.insert(
            0,
            {
                "type": "email",
                "value": best_contact["email"],
                "source_url": best_contact["source_url"],
                "publicly_listed": True,
                "authorized": True,
                "confidence": contact_quality(
                    best_contact["email"],
                    best_contact["source_type"],
                    best_contact.get("context", ""),
                )
                / 10,
            },
        )
    if not contact_points:
        return None
    documents = adapter.fetch(candidate, DiscoveryContext(deterministic=False))
    evidence = adapter.normalize(documents, DiscoveryContext(deterministic=False))
    evidence_text = " ".join(item.claim for item in evidence) or page_text or str(candidate.metadata.get("description") or "")
    project_url = candidate.url
    return normalize_prospect(
        {
            "name": (
                best_contact["email"].partition("@")[0].replace(".", " ").title()
                if best_contact
                else candidate.title or "Project team"
            ),
            "email": best_contact["email"] if best_contact else "",
            "email_source_url": (
                best_contact["source_url"] if best_contact else contact_points[0]["source_url"]
            ),
            "email_source_type": best_contact["source_type"] if best_contact and best_contact["source_type"] in {
                "github_profile",
                "repository_readme",
                "official_website",
                "project_docs",
                "package_page",
            } else "official_website",
            "contact_points": contact_points,
            "project": candidate.title or urllib.parse.urlparse(project_url).netloc,
            "project_url": project_url,
            "project_description": str(candidate.metadata.get("description") or candidate.title or ""),
            "category": category or category_for(evidence_text),
            "research_text": evidence_text,
            "evidence_urls": [
                candidate.url,
                best_contact["source_url"] if best_contact else contact_points[0]["source_url"],
                *[item.source_url for item in evidence],
            ],
            "owner_login": urllib.parse.urlparse(project_url).netloc,
            "owner_type": "",
            "updated_at": candidate.published_at or utc_now(),
        }
    )


def discover_from_adapters(
    registry: Any,
    target: int,
    category: str | None,
    seen_projects: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    prospects: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    context = DiscoveryContext(deterministic=bool_env("OUTREACH_ADAPTER_FIXTURES"))
    adapters = [
        adapter
        for adapter in registry.enabled()
        if adapter.health_check().status != "disabled" and adapter.source_type != "github"
    ]
    terms: list[str] = []
    for pack in query_pack_order(category):
        terms.extend(pack.get("registry", [])[:2])
        terms.extend(pack.get("github", [])[:1])
    for adapter in adapters:
        for term in list(dict.fromkeys(terms))[:6]:
            if len(prospects) >= target:
                return prospects, signals
            query = DiscoveryQuery(text=term, category=category, limit=4)
            try:
                candidates = adapter.discover(query, context)
            except Exception as error:
                signals.append({"source_type": adapter.source_type, "status": "degraded", "error": str(error)})
                continue
            if hasattr(adapter, "drain_errors"):
                for error in adapter.drain_errors():
                    signals.append(
                        {
                            "source_type": error.source_type,
                            "status": "degraded",
                            "operation": error.operation,
                            "error_category": error.category,
                            "error": error.message,
                            "retryable": error.retryable,
                            "attempt": error.attempt,
                            "occurred_at": error.occurred_at,
                        }
                    )
            for candidate in candidates:
                if len(prospects) >= target:
                    break
                documents = adapter.fetch(candidate, context)
                if hasattr(adapter, "drain_errors"):
                    for error in adapter.drain_errors():
                        signals.append(
                            {
                                "source_type": error.source_type,
                                "status": "degraded",
                                "operation": error.operation,
                                "error_category": error.category,
                                "error": error.message,
                                "retryable": error.retryable,
                                "attempt": error.attempt,
                                "occurred_at": error.occurred_at,
                            }
                        )
                evidence = adapter.normalize(documents, context)
                signals.append(
                    {
                        "source_type": candidate.source_type,
                        "source_id": candidate.source_id,
                        "url": candidate.url,
                        "title": candidate.title,
                        "evidence_count": len(evidence),
                        "repository_url": str(candidate.metadata.get("repository_url") or ""),
                        "official_url": str(candidate.metadata.get("official_url") or ""),
                        "independence_groups": sorted({item.independence_group for item in evidence}),
                    }
                )
                if not evidence:
                    continue
                prospect = prospect_from_source_candidate(adapter, candidate, category, seen_projects)
                if prospect:
                    prospects.append(prospect)
    return prospects, signals


def discover(
    target: int,
    input_path: Path | None,
    category: str | None,
    registry: Any | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    excluded_emails = load_json_env_list("OUTREACH_EXCLUDED_EMAILS")
    excluded_domains = load_json_env_list("OUTREACH_EXCLUDED_DOMAINS")
    excluded_project_keys = load_json_env_list("OUTREACH_EXCLUDED_PROJECT_KEYS")
    allow_generic_email = bool_env("ALLOW_GENERIC_CONTACT_EMAILS")
    allow_large_company = bool_env("ALLOW_LARGE_COMPANY_RESULTS")
    memory = load_memory()
    prospects = load_seed(input_path)
    if category:
        prospects = [prospect for prospect in prospects if prospect["category"] == category]
    adapter_signals: list[dict[str, Any]] = []
    seen_adapter_projects: set[str] = {prospect["project"].strip().lower() for prospect in prospects}
    if registry and len(prospects) < target:
        adapter_prospects, adapter_signals = discover_from_adapters(
            registry,
            max(target * 2, target - len(prospects)),
            category,
            seen_adapter_projects,
        )
        prospects.extend(adapter_prospects)
    if len(prospects) < target:
        prospects.extend(discover_from_github(max(target * 3, target - len(prospects)), category))
    unique: dict[str, dict[str, Any]] = {}
    skipped: list[dict[str, Any]] = []
    domains: Counter[str] = Counter()
    owners: Counter[str] = Counter()
    categories: Counter[str] = Counter()
    generic_email_count = 0
    large_company_count = 0
    for prospect in prospects:
        email = prospect.get("email", "").lower()
        domain = email.split("@")[-1] if email else ""
        contact_key = email or "|".join(
            f"{item.get('type')}:{str(item.get('value', '')).lower()}"
            for item in prospect.get("contact_points", [])
            if isinstance(item, dict)
        )
        project_key = prospect["project"].strip().lower()
        diagnostics = qualification_diagnostics(prospect)
        duplicate_reason = ""
        if (
            email in excluded_emails
            or domain in excluded_domains
            or project_key in excluded_project_keys
            or (email and email in unique)
            or (email and email in memory["emails"])
            or owner_key(prospect.get("owner_login", "")) in memory["owners"]
            or prospect["project_url"].strip().lower() in memory["repos"]
            or normalize_name(prospect["name"]) in memory["names"]
        ):
            duplicate_reason = "duplicate_or_previously_seen"
        if duplicate_reason:
            diagnostics.update(
                {
                    "excluded": True,
                    "skip_reason": duplicate_reason,
                    "exclusion_rule_triggered": duplicate_reason,
                    "duplicate": True,
                }
            )
        if diagnostics["excluded"] and (diagnostics["generic"] and allow_generic_email):
            diagnostics["excluded"] = False
            diagnostics["skip_reason"] = ""
            diagnostics["exclusion_rule_triggered"] = ""
        if diagnostics["large_company"] and allow_large_company:
            diagnostics["excluded"] = False
            diagnostics["skip_reason"] = ""
            diagnostics["exclusion_rule_triggered"] = ""
        owner = diagnostics["owner_key"]
        if not diagnostics["excluded"] and owners[owner] >= 1:
            diagnostics.update(
                {
                    "excluded": True,
                    "skip_reason": "owner_diversity_cap",
                    "exclusion_rule_triggered": "owner_diversity_cap",
                }
            )
        if not diagnostics["excluded"] and categories[prospect["category"]] >= 2:
            diagnostics.update(
                {
                    "excluded": True,
                    "skip_reason": "category_diversity_cap",
                    "exclusion_rule_triggered": "category_diversity_cap",
                }
            )
        if not diagnostics["excluded"] and diagnostics["generic"] and generic_email_count >= 1:
            diagnostics.update(
                {
                    "excluded": True,
                    "skip_reason": "generic_email_cap",
                    "exclusion_rule_triggered": "generic_email_cap",
                }
            )
        if not diagnostics["excluded"] and diagnostics["large_company"] and large_company_count >= 1:
            diagnostics.update(
                {
                    "excluded": True,
                    "skip_reason": "large_company_cap",
                    "exclusion_rule_triggered": "large_company_cap",
                }
            )
        if diagnostics["excluded"]:
            skipped.append(
                {
                    "prospect_id": prospect["prospect_id"],
                    "project": prospect["project"],
                    "email": prospect["email"],
                    "category": prospect["category"],
                    **diagnostics,
                }
            )
            continue
        prospect["diagnostics"] = diagnostics
        unique[contact_key or project_key] = prospect
        if domain:
            domains[domain] += 1
        owners[owner] += 1
        categories[prospect["category"]] += 1
        if diagnostics["generic"]:
            generic_email_count += 1
        if diagnostics["large_company"]:
            large_company_count += 1
        if len(unique) >= target:
            break
    accepted = list(unique.values())[:target]
    persist_memory(accepted)
    return accepted, skipped, adapter_signals


def pick_detail(text: str, fallback: str) -> str:
    clean = clean_research_text(text)
    sentences = split_sentences(clean)
    for sentence in sentences:
        if 35 <= len(sentence) <= 240 and CONCRETE_WORKLOAD_RE.search(sentence):
            return sentence
    return fallback or "the project documents durable AI workload execution"


def research(prospects: list[dict[str, Any]]) -> list[dict[str, Any]]:
    notes = []
    for prospect in prospects:
        diagnostics = prospect.get("diagnostics", qualification_diagnostics(prospect))
        evidence_points = diagnostics["evidence_points"] or [
            pick_detail(prospect["research_text"], prospect["project_description"] or prospect["project"])
        ]
        pain_signals = diagnostics["pain_signals"]
        pain_signal = next(
            (signal for signal in pain_signals if signal.lower() != evidence_points[0].lower()),
            pain_signals[0] if pain_signals else evidence_points[0],
        )
        structured_evidence = structured_evidence_for_prospect(prospect, diagnostics, evidence_points, pain_signals)
        graph = canonical_entity_graph(prospect, structured_evidence)
        prospect.update(graph)
        summary_parts = evidence_points[:2]
        strength = max(0.0, min(1.0, float(diagnostics.get("contact_quality", 0)) / 10 * 0.15 + 0.25 + 0.25 * len(evidence_points) + (0.15 if diagnostics.get("small_team_context") else 0) + (0.2 if not diagnostics.get("stale") else 0)))
        notes.append(
            {
                "prospect_id": prospect["prospect_id"],
                "entity_id": prospect.get("canonical_entity_id") or prospect["entity_id"],
                "summary": clip_words(f"{prospect['project']} shows {'. '.join(summary_parts)}", 45),
                "personalization_detail": clip_words(evidence_points[0], 20),
                "junglegrid_relevance": clip_words(
                    f"Likely fit for {CAMPAIGN['offer']['name']} because {pain_signal} matches the configured campaign signals.",
                    28,
                ),
                "campaign_relevance": clip_words(
                    f"Likely fit for {CAMPAIGN['offer']['name']} because {pain_signal} matches the configured campaign signals.",
                    28,
                ),
                "evidence_urls": prospect["evidence_urls"],
                "evidence_strength": round(strength, 2),
                "evidence_points": evidence_points,
                "pain_signals": pain_signals,
                "evidence": structured_evidence,
                "junglegrid_job_id": os.getenv(
                    "JUNGLEGRID_JOB_ID", "fixture-job"
                ),
            }
        )
    return notes


def score_breakdown(prospect: dict[str, Any], note: dict[str, Any]) -> dict[str, int]:
    text = clean_research_text(
        " ".join(
            [
                prospect["project"],
                prospect["project_description"],
                note["summary"],
                note["personalization_detail"],
                " ".join(note.get("evidence_points", [])),
            ]
        )
    )
    evidence_points = note.get("evidence_points", [])
    pain_signals = note.get("pain_signals", [])
    diagnostics = prospect.get("diagnostics", {})
    contact = int(diagnostics.get("contact_quality", 0))
    updated_days = diagnostics.get("updated_days")
    target_match = bool(campaign_regex("targetTerms").search(text))
    workload_match = bool(campaign_regex("workloadTerms").search(text))
    agent = 20 if target_match and len(evidence_points) >= 2 else (12 if target_match and evidence_points else 0)
    workload = (
        20
        if workload_match and len(evidence_points) >= 2
        else (12 if workload_match and evidence_points else 0)
    )
    infrastructure = 20 if pain_signals else 0
    activity = 15 if updated_days is not None and updated_days <= 30 else (11 if updated_days is not None and updated_days <= 90 else 4)
    comprehension = (
        15
        if len(evidence_points) >= 2 and diagnostics.get("has_execution_surface")
        else (8 if len(evidence_points) >= 2 else 0)
    )
    return {
        "agentMcpRelevance": min(20, agent),
        "aiWorkloadRelevance": min(20, workload),
        "infrastructurePain": min(20, infrastructure),
        "openSourceActivity": min(15, activity),
        "jungleGridComprehension": min(15, comprehension),
        "contactQuality": min(10, contact),
    }


def score(prospects: list[dict[str, Any]], notes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id = {note["prospect_id"]: note for note in notes}
    rows = []
    for prospect in prospects:
        note = by_id[prospect["prospect_id"]]
        breakdown = score_breakdown(prospect, note)
        public = {key: value for key, value in prospect.items() if key not in {"research_text", "evidence_urls", "stars", "active"}}
        diagnostics = prospect.get("diagnostics", {})
        evidence_points = note.get("evidence_points", [])
        evidence_items = note.get("evidence", [])
        concrete_pain_signal = (note.get("pain_signals") or evidence_points or [prospect["project_description"]])[0]
        fit_score = sum(breakdown.values())
        independent_evidence_count = max(0, min(3, len(evidence_points)))
        if independent_evidence_count <= 1:
            fit_score = min(fit_score, 50)
        elif independent_evidence_count == 2:
            fit_score = min(fit_score, 70)
        if fit_score >= 90 and (
            len(evidence_points) < 3
            or not diagnostics.get("has_direct_ai_workload")
            or not diagnostics.get("has_execution_surface")
            or diagnostics.get("contact_quality", 0) < 8
        ):
            fit_score = 89
        rows.append(
            {
                **public,
                "fit_score": fit_score,
                "score_breakdown": breakdown,
                "evidence_strength": note["evidence_strength"],
                "evidence": evidence_items,
                "score_evidence_ids": {
                    "agentMcpRelevance": [
                        item["evidence_id"] for item in evidence_items if item.get("claim_type") in {"ai_workload", "target_workload", "product_fit", "integration_surface"}
                    ][:3],
                    "aiWorkloadRelevance": [
                        item["evidence_id"] for item in evidence_items if item.get("claim_type") in {"ai_workload", "target_workload"}
                    ][:3],
                    "infrastructurePain": [
                        item["evidence_id"] for item in evidence_items if item.get("claim_type") == "infrastructure_pain"
                    ][:3],
                    "openSourceActivity": [
                        item["evidence_id"] for item in evidence_items if item.get("claim_type") == "activity"
                    ][:2],
                    "jungleGridComprehension": [
                        item["evidence_id"] for item in evidence_items if item.get("claim_type") == "integration_surface"
                    ][:2],
                    "contactQuality": [
                        item["evidence_id"] for item in evidence_items if item.get("claim_type") == "contact"
                    ][:2],
                },
                "contact_quality": diagnostics.get("contact_quality", 0),
                "evidence_points": evidence_points,
                "why_this_person": clip_words(
                    f"{prospect['name']} appears to be a reachable maintainer or builder for {prospect['project']}.",
                    20,
                ),
                "why_now": clip_words(
                    f"The repo is active and currently surfaces execution concerns like {concrete_pain_signal}.",
                    20,
                ),
                "concrete_pain_signal": concrete_pain_signal,
                "suggested_angle": clip_words(
                    note.get("semantic_suggested_angle") or CAMPAIGN["messaging"]["positioning"],
                    18,
                ),
                "score_explanation": note.get("semantic_score_explanation")
                or clip_words(
                    f"The score is grounded in {len(evidence_items)} structured evidence items and the configured campaign signals.",
                    30,
                ),
                "outreach_priority": "high" if fit_score >= 85 else ("medium" if fit_score >= 75 else "low"),
                "excluded": False,
                "junglegrid_job_id": os.getenv(
                    "JUNGLEGRID_JOB_ID", "fixture-job"
                ),
                "proof_artifacts": [
                    {
                        "type": CAMPAIGN.get("proofOfValue", {}).get(
                            "strategy", "implementation_plan"
                        ),
                        "title": (
                            f"{CAMPAIGN['offer']['name']} proof of value for "
                            f"{prospect['project']}"
                        ),
                        "content": (
                            f"Validate the evidenced need for {prospect['project']}, "
                            f"connect {CAMPAIGN['offer']['name']} to the current workflow, "
                            "run a bounded pilot, and compare reliability, delivery time, "
                            "and operating cost."
                        ),
                        "uri": None,
                        "evidence_ids": [
                            item["evidence_id"]
                            for item in evidence_items
                            if item.get("evidence_id")
                        ],
                        "junglegrid_job_id": os.getenv(
                            "JUNGLEGRID_JOB_ID", "fixture-job"
                        ),
                    }
                ],
            }
        )
    return rows


def template_draft(prospect: dict[str, Any], note: dict[str, Any]) -> tuple[str, str, list[str]]:
    first_name = prospect["name"].split()[0] if prospect["name"].strip() else "there"
    project_name = prospect["project"].split("/")[-1]
    detail = clip_words(note["personalization_detail"], 14)
    pain = clip_words((note.get("pain_signals") or [note["personalization_detail"]])[0], 14)
    offer = CAMPAIGN["offer"]
    messaging = CAMPAIGN["messaging"]
    if CAMPAIGN.get("campaignId") == "jungle-grid" and CONTROL_PLANE_RE.search(
        f"{note.get('summary', '')} {note.get('personalization_detail', '')}"
    ):
        angle = "Jungle Grid can sit underneath that control plane as an execution target for heavier jobs."
    elif CAMPAIGN.get("campaignId") == "jungle-grid" and re.search(
        r"\b(gpu|inference|model serving|vllm|fine[- ]?tun)", pain, re.I
    ):
        angle = "Jungle Grid is meant to add routed compute capacity without making you own the execution layer."
    else:
        angle = messaging["positioning"]
    body = (
        f"Hi {first_name},\n\n"
        f"I read the public docs for {project_name} and noticed {detail}. "
        f"I’m building {offer['name']}: {offer['description']} {angle}\n\n"
        f"The reason I reached out is that {pain}. That detail matches the audience and operating signals configured for this campaign.\n\n"
        f"{messaging['callToAction']} {SITE}.\n\n"
        f"{offer['signature']}"
    )
    if word_count(body) < MIN_WORDS:
        body = body.replace(
            "The reason I reached out is that",
            "The reason I reached out is that, in practice, teams usually hit friction around observability and job durability once usage grows, and",
        )
    if word_count(body) > MAX_WORDS:
        detail = clip_words(note["personalization_detail"], 10)
        return template_draft(prospect, {**note, "personalization_detail": detail, "pain_signals": [pain]})
    return f"{messaging['subjectPrefix']} {project_name}"[:MAX_SUBJECT], body, [detail]


def system_prompt() -> str:
    return (
        "You write concise founder-led outreach emails using only the provided evidence and campaign configuration. "
        f"Do not invent facts. Output plain text only. Include exactly one link and it must be {SITE}. "
        "Keep the email under 140 words. If evidence is insufficient, return SKIP."
    )


def ollama_base() -> str:
    return os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434").rstrip("/")


def wait_for_ollama(timeout_seconds: int = 25) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            request_json(f"{ollama_base()}/api/tags", timeout=3)
            return True
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            time.sleep(1)
    return False


def ensure_ollama(model: str) -> bool:
    global OLLAMA_PROCESS
    if not wait_for_ollama(2):
        host = urllib.parse.urlparse(ollama_base())
        if host.hostname not in {"127.0.0.1", "localhost"}:
            return False
        try:
            OLLAMA_PROCESS = subprocess.Popen(
                ["ollama", "serve"],
                stdout=sys.stderr,
                stderr=sys.stderr,
                start_new_session=True,
            )
        except (FileNotFoundError, OSError) as error:
            LOG.warning("Ollama could not be started: %s", error)
            return False
        if not wait_for_ollama():
            return False
    try:
        tags = request_json(f"{ollama_base()}/api/tags", timeout=10)
        names = {str(item.get("name", "")) for item in tags.get("models", [])}
        if model not in names and not any(name.startswith(f"{model}:") for name in names):
            LOG.info("Pulling Ollama model %s.", model)
            request_json(
                f"{ollama_base()}/api/pull",
                method="POST",
                payload={"name": model, "stream": False},
                timeout=900,
            )
    except (urllib.error.URLError, TimeoutError) as error:
            LOG.warning("Ollama model pull failed: %s", error)
            return False
    return True


def stop_ollama() -> None:
    global OLLAMA_PROCESS
    if OLLAMA_PROCESS is None:
        return
    process = OLLAMA_PROCESS
    OLLAMA_PROCESS = None
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def qwen_draft(
    prospect: dict[str, Any],
    note: dict[str, Any],
    model: str,
) -> tuple[str, str, list[str]] | None:
    prompt = {
        "campaign": CAMPAIGN,
        "recipient": {"name": prospect["name"], "project": prospect["project"]},
        "public_evidence": {
            "detail": note["personalization_detail"],
            "summary": note["summary"],
            "relevance": note["junglegrid_relevance"],
            "urls": note["evidence_urls"],
        },
        "output": {
            "format": "JSON",
            "fields": ["subject", "body", "personalization_claims"],
            "signature": CAMPAIGN["offer"]["signature"],
        },
    }
    response = request_json(
        f"{ollama_base()}/api/generate",
        method="POST",
        payload={
            "model": model,
            "system": system_prompt(),
            "prompt": json.dumps(prompt),
            "format": "json",
            "stream": False,
            "options": {"temperature": 0.2},
        },
        timeout=180,
    )
    raw = str(response.get("response", "")).strip()
    if raw.upper() == "SKIP" or not raw:
        return None
    generated = json.loads(raw)
    if generated.get("skip") is True:
        return None
    return (
        str(generated.get("subject", "")).strip(),
        str(generated.get("body", "")).strip(),
        [str(claim).strip() for claim in generated.get("personalization_claims", []) if str(claim).strip()],
    )


def parse_qwen_json_response(response: dict[str, Any]) -> Any:
    raw = str(response.get("response", "")).strip()
    if not raw:
        raise ValueError("Qwen returned an empty response.")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```", raw, re.I | re.S)
        if match:
            return json.loads(match.group(1))
        start_candidates = [index for index in (raw.find("{"), raw.find("[")) if index >= 0]
        end_candidates = [index for index in (raw.rfind("}"), raw.rfind("]")) if index >= 0]
        if start_candidates and end_candidates:
            start = min(start_candidates)
            end = max(end_candidates)
            if end > start:
                return json.loads(raw[start : end + 1])
        raise


def unwrap_qwen_object(value: Any, required_key: str | None = None) -> dict[str, Any]:
    if isinstance(value, dict) and (required_key is None or required_key in value):
        return value
    if isinstance(value, list) and required_key == "items":
        return {"items": value}
    if isinstance(value, dict):
        for key in ("result", "analysis", "output", "data", "response"):
            nested = value.get(key)
            if isinstance(nested, dict) and (required_key is None or required_key in nested):
                return nested
            if isinstance(nested, list) and required_key == "items":
                return {"items": nested}
        for nested in value.values():
            if isinstance(nested, dict) and (required_key is None or required_key in nested):
                return nested
            if isinstance(nested, list) and required_key == "items":
                return {"items": nested}
    raise ValueError(
        f"Qwen response did not return a JSON object containing {required_key}."
        if required_key
        else "Qwen response did not return a JSON object."
    )


def qwen_semantic_analysis(
    prospects: list[dict[str, Any]],
    notes: list[dict[str, Any]],
    model: str,
) -> dict[str, dict[str, Any]]:
    notes_by_id = {note["prospect_id"]: note for note in notes}
    prompt = {
        "campaign": CAMPAIGN,
        "task": (
            "Analyze each prospect using only the supplied evidence. Decide whether the campaign "
            "qualification is supported, summarize the research, explain the evidence-bound score, "
            "and select a non-fabricated outreach angle."
        ),
        "prospects": [
            {
                "prospect_id": prospect["prospect_id"],
                "project": prospect["project"],
                "category": prospect["category"],
                "description": prospect["project_description"],
                "deterministic_qualification": prospect.get("diagnostics", {}),
                "research": notes_by_id[prospect["prospect_id"]],
            }
            for prospect in prospects
        ],
        "output": {
            "format": "JSON",
            "shape": {
                "items": [
                    {
                        "prospect_id": "string",
                        "qualified": "boolean",
                        "qualification_reason": "string",
                        "research_analysis": "string",
                        "score_explanation": "string",
                        "suggested_angle": "string",
                    }
                ]
            },
        },
    }
    response = request_json(
        f"{ollama_base()}/api/generate",
        method="POST",
        payload={
            "model": model,
            "system": (
                "You are a strict evidence analyst. Do not add facts absent from the supplied "
                "evidence. Return valid JSON for every prospect ID."
            ),
            "prompt": json.dumps(prompt),
            "format": "json",
            "stream": False,
            "options": {"temperature": 0.1},
        },
        timeout=240,
    )
    generated = unwrap_qwen_object(parse_qwen_json_response(response), "items")
    items = generated.get("items")
    if not isinstance(items, list):
        raise ValueError("Semantic analysis did not return an items array.")
    expected = {prospect["prospect_id"] for prospect in prospects}
    results: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        prospect_id = str(item.get("prospect_id", ""))
        if prospect_id not in expected:
            continue
        required_text = [
            str(item.get("qualification_reason", "")).strip(),
            str(item.get("research_analysis", "")).strip(),
            str(item.get("score_explanation", "")).strip(),
            str(item.get("suggested_angle", "")).strip(),
        ]
        if not all(required_text) or not isinstance(item.get("qualified"), bool):
            raise ValueError(f"Semantic analysis is incomplete for {prospect_id}.")
        results[prospect_id] = {
            "qualified": item["qualified"],
            "qualification_reason": clip_words(required_text[0], 28),
            "research_analysis": clip_words(required_text[1], 45),
            "score_explanation": clip_words(required_text[2], 45),
            "suggested_angle": clip_words(required_text[3], 28),
        }
    if set(results) != expected:
        raise ValueError("Semantic analysis omitted one or more prospects.")
    return results


def apply_semantic_analysis(
    prospects: list[dict[str, Any]],
    notes: list[dict[str, Any]],
    analysis: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    notes_by_id = {note["prospect_id"]: note for note in notes}
    accepted: list[dict[str, Any]] = []
    accepted_notes: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for prospect in prospects:
        result = analysis[prospect["prospect_id"]]
        note = notes_by_id[prospect["prospect_id"]]
        note["semantic_research_analysis"] = result["research_analysis"]
        note["semantic_qualification_reason"] = result["qualification_reason"]
        note["semantic_score_explanation"] = result["score_explanation"]
        note["semantic_suggested_angle"] = result["suggested_angle"]
        if not result["qualified"]:
            excluded.append(
                {
                    "prospect_id": prospect["prospect_id"],
                    "project": prospect["project"],
                    "skip_reason": "semantic_qualification_rejected",
                    "exclusion_rule_triggered": "semantic_qualification_rejected",
                    "missing_evidence": [result["qualification_reason"]],
                }
            )
            continue
        prospect["semantic_qualified"] = True
        accepted.append(prospect)
        accepted_notes.append(note)
    return accepted, accepted_notes, excluded


def qwen_semantic_validate_drafts(
    drafts: list[dict[str, Any]],
    notes: list[dict[str, Any]],
    model: str,
) -> dict[str, dict[str, Any]]:
    notes_by_id = {note["prospect_id"]: note for note in notes}
    prompt = {
        "campaign": CAMPAIGN,
        "task": (
            "Semantically validate each draft. Reject unsupported claims, malformed personalization, "
            "irrelevant prospects, relationship mismatches, contaminated evidence, and incorrect offer angles."
        ),
        "drafts": [
            {
                "prospect_id": draft["prospect_id"],
                "subject": draft["subject"],
                "body": draft["body"],
                "claims": draft["personalization_claims"],
                "evidence": notes_by_id[draft["prospect_id"]],
            }
            for draft in drafts
        ],
        "output": {
            "format": "JSON",
            "shape": {
                "items": [
                    {
                        "prospect_id": "string",
                        "status": "send_ready|manual_review_required|regeneration_required|excluded",
                        "reasons": ["string"],
                    }
                ]
            },
        },
    }
    response = request_json(
        f"{ollama_base()}/api/generate",
        method="POST",
        payload={
            "model": model,
            "system": "You are a fail-closed outreach semantic validator. Return valid JSON only.",
            "prompt": json.dumps(prompt),
            "format": "json",
            "stream": False,
            "options": {"temperature": 0.0},
        },
        timeout=240,
    )
    generated = unwrap_qwen_object(parse_qwen_json_response(response), "items")
    items = generated.get("items")
    if not isinstance(items, list):
        raise ValueError("Semantic validation did not return an items array.")
    allowed = {"send_ready", "manual_review_required", "regeneration_required", "excluded"}
    expected = {draft["prospect_id"] for draft in drafts}
    results: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        prospect_id = str(item.get("prospect_id", ""))
        status = str(item.get("status", ""))
        reasons = [str(reason).strip() for reason in item.get("reasons", []) if str(reason).strip()]
        if prospect_id not in expected or status not in allowed:
            continue
        if status != "send_ready" and not reasons:
            raise ValueError(f"Semantic validation omitted reasons for {prospect_id}.")
        results[prospect_id] = {"status": status, "reasons": reasons}
    if set(results) != expected:
        raise ValueError("Semantic validation omitted one or more drafts.")
    return results


def qwen_conversation_turn(payload: dict[str, Any], model: str) -> dict[str, Any]:
    trigger = str(payload.get("trigger") or "inbound_reply")
    task = (
        "Evaluate the scheduled follow-up against the conversation history and current "
        "campaign policy. Choose whether to respond now, follow up later, escalate, or close."
        if trigger == "scheduled_follow_up"
        else "Classify the inbound reply and choose the next conversation action."
    )
    response = request_json(
        f"{ollama_base()}/api/generate",
        method="POST",
        payload={
            "model": model,
            "system": (
                "You manage an evidence-bound business-development conversation. "
                "Use only the supplied campaign, prospect, evidence, and message history. "
                "Classify the inbound reply, update the conversation state, choose the next "
                "action, and draft a concise response when appropriate. Return valid JSON only."
            ),
            "prompt": json.dumps(
                {
                    "campaign": CAMPAIGN,
                    "conversation": payload,
                    "task": task,
                    "output": {
                        "classification": (
                            "interested|question|objection|not_now|opt_out|"
                            "wrong_person|other"
                        ),
                        "summary": "string",
                        "open_questions": ["string"],
                        "commitments": ["string"],
                        "objections": ["string"],
                        "follow_up_at": "ISO datetime or null",
                        "opportunity_state": (
                            "qualified|engaged|evaluating|committed|won|lost"
                        ),
                        "next_action": "respond|follow_up_later|escalate|close",
                        "response_subject": "string or null",
                        "response_body": "string or null",
                        "escalation_required": "boolean",
                    },
                }
            ),
            "format": "json",
            "stream": False,
            "options": {"temperature": 0.1},
        },
        timeout=240,
    )
    generated = unwrap_qwen_object(parse_qwen_json_response(response))
    allowed_classifications = {
        "interested",
        "question",
        "objection",
        "not_now",
        "opt_out",
        "wrong_person",
        "other",
    }
    allowed_states = {
        "qualified",
        "engaged",
        "evaluating",
        "committed",
        "won",
        "lost",
    }
    allowed_actions = {"respond", "follow_up_later", "escalate", "close"}
    classification = str(generated.get("classification", ""))
    opportunity_state = str(generated.get("opportunity_state", ""))
    next_action = str(generated.get("next_action", ""))
    if (
        classification not in allowed_classifications
        or opportunity_state not in allowed_states
        or next_action not in allowed_actions
        or not str(generated.get("summary", "")).strip()
        or not isinstance(generated.get("escalation_required"), bool)
    ):
        raise ValueError("Conversation analysis returned an invalid contract.")
    inbound = str(payload.get("inbound_body", ""))
    if re.search(
        r"\b(unsubscribe|opt\s*out|stop contacting|do not contact|remove me)\b",
        inbound,
        re.I,
    ):
        classification = "opt_out"
        opportunity_state = "lost"
        next_action = "close"
        generated["response_subject"] = None
        generated["response_body"] = None
    return {
        "schema_version": "1.0",
        "classification": classification,
        "summary": str(generated["summary"]).strip(),
        "open_questions": [
            str(value).strip()
            for value in generated.get("open_questions", [])
            if str(value).strip()
        ],
        "commitments": [
            str(value).strip()
            for value in generated.get("commitments", [])
            if str(value).strip()
        ],
        "objections": [
            str(value).strip()
            for value in generated.get("objections", [])
            if str(value).strip()
        ],
        "follow_up_at": generated.get("follow_up_at"),
        "opportunity_state": opportunity_state,
        "next_action": next_action,
        "response_subject": (
            str(generated.get("response_subject", "")).strip() or None
        ),
        "response_body": str(generated.get("response_body", "")).strip() or None,
        "escalation_required": bool(generated["escalation_required"]),
    }


def qwen_validate_conversation_response(
    payload: dict[str, Any], result: dict[str, Any], model: str
) -> tuple[str, list[str]]:
    if not result.get("response_body"):
        return (
            "excluded"
            if result["next_action"] == "close"
            else "manual_review_required",
            [] if result["next_action"] == "close" else ["no outbound response generated"],
        )
    response = request_json(
        f"{ollama_base()}/api/generate",
        method="POST",
        payload={
            "model": model,
            "system": (
                "You are a fail-closed semantic validator for a business-development "
                "conversation. Return valid JSON only."
            ),
            "prompt": json.dumps(
                {
                    "campaign": CAMPAIGN,
                    "conversation": payload,
                    "analysis": result,
                    "task": (
                        "Validate that the response is supported, relevant, respectful, "
                        "channel-appropriate, and does not ignore an opt-out or escalation."
                    ),
                    "output": {
                        "status": (
                            "send_ready|manual_review_required|"
                            "regeneration_required|excluded"
                        ),
                        "reasons": ["string"],
                    },
                }
            ),
            "format": "json",
            "stream": False,
            "options": {"temperature": 0.0},
        },
        timeout=240,
    )
    generated = unwrap_qwen_object(parse_qwen_json_response(response))
    status = str(generated.get("status", ""))
    reasons = [
        str(reason).strip()
        for reason in generated.get("reasons", [])
        if str(reason).strip()
    ]
    if status not in {
        "send_ready",
        "manual_review_required",
        "regeneration_required",
        "excluded",
    }:
        raise ValueError("Conversation validation returned an invalid status.")
    if status != "send_ready" and not reasons:
        raise ValueError("Conversation validation omitted failure reasons.")
    return status, reasons


def run_conversation_turn(output: Path) -> int:
    raw = os.getenv("OPENLINE_CONVERSATION_INPUT", "").strip()
    if not raw:
        raise ValueError("OPENLINE_CONVERSATION_INPUT is required.")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("Conversation input must be an object.")
    trigger = str(payload.get("trigger") or "inbound_reply")
    if trigger not in {"inbound_reply", "scheduled_follow_up"}:
        raise ValueError("Conversation input has an unsupported trigger.")
    if trigger == "inbound_reply" and not str(payload.get("inbound_body", "")).strip():
        raise ValueError("Inbound conversation input must include inbound_body.")
    model = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")
    if not ensure_ollama(model):
        raise RuntimeError("Qwen/Ollama is unavailable for the conversation turn.")
    result = qwen_conversation_turn(payload, model)
    validation_status, validation_reasons = qwen_validate_conversation_response(
        payload, result, model
    )
    result["validation_status"] = validation_status
    result["validation_reasons"] = validation_reasons
    write_json(output, "conversation_result.json", result)
    return 0


def apply_semantic_draft_validation(
    drafts: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    validation: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    next_failures = list(failures)
    for draft in drafts:
        if draft["prospect_id"] not in validation:
            accepted.append(draft)
            continue
        result = validation[draft["prospect_id"]]
        draft["validation_status"] = result["status"]
        draft["validation_errors"] = result["reasons"]
        if result["status"] == "send_ready":
            accepted.append(draft)
        else:
            next_failures.append(
                {
                    "prospect_id": draft["prospect_id"],
                    "validation_status": result["status"],
                    "errors": result["reasons"],
                }
            )
    return accepted, next_failures


def build_draft_candidate(
    prospect: dict[str, Any],
    note: dict[str, Any],
    subject: str,
    body: str,
    claims: list[str],
    model_mode: str,
) -> dict[str, Any]:
    return {
        "prospect_id": prospect["prospect_id"],
        "name": prospect["name"],
        "email": prospect["email"],
        "email_source_url": prospect["email_source_url"],
        "project": prospect["project"],
        "category": prospect["category"],
        "fit_score": prospect["fit_score"],
        "subject": subject,
        "body": body,
        "word_count": word_count(body),
        "links": [link.rstrip(".,;:!?") for link in URL_RE.findall(f"{subject}\n{body}")],
        "evidence_urls": note["evidence_urls"],
        "personalization_claims": claims,
        "model_mode": model_mode,
        "validation_status": "manual_review_required" if model_mode == "fallback" else "send_ready",
        "validation_errors": [],
    }


def validate_draft(draft: dict[str, Any], max_per_domain: int, domains: Counter[str]) -> list[str]:
    errors: list[str] = []
    body = draft["body"]
    evidence_text = " ".join([body, *draft.get("personalization_claims", [])])
    links = [link.rstrip(".,;:!?") for link in URL_RE.findall(f"{draft['subject']}\n{body}")]
    count = word_count(body)
    domain = draft["email"].split("@")[-1].lower()
    if count < MIN_WORDS or count > MAX_WORDS:
        errors.append(f"body must contain {MIN_WORDS}-{MAX_WORDS} words; found {count}")
    if len(draft["subject"]) > MAX_SUBJECT:
        errors.append("subject must be under 80 characters")
    if links != ALLOWED_LINKS:
        errors.append(f"draft must contain exactly one link: {SITE}")
    if re.search(r"<(?:img|a|script|style|html|body)\b|tracking\s*pixel|utm_|unsubscribe|open tracking", body, re.I):
        errors.append("tracking and HTML are not allowed")
    if re.search(r"\battachment\b", body, re.I):
        errors.append("attachments are not allowed")
    if not draft["email_source_url"]:
        errors.append("email source URL is required")
    if draft["email_source_url"] not in draft["evidence_urls"]:
        errors.append("email source URL must be included in evidence URLs")
    if not draft["personalization_claims"]:
        errors.append("at least one evidence-bound personalization claim is required")
    if draft.get("model_mode") == "fallback":
        draft["validation_status"] = "manual_review_required"
        draft["validation_errors"] = unique_preserve_order(
            [*draft.get("validation_errors", []), "fallback generation requires manual review"]
        )
    if contamination_reasons(evidence_text):
        errors.append("draft contains contaminated evidence")
    if re.search(r"\bi noticed\b\s*(?:$|[.,;:])", body, re.I):
        errors.append("personalization is incomplete after 'I noticed'")
    if re.search(r"\bnoticed\s+[a-z0-9_.-]+/[a-z0-9_.-]+\b", body, re.I):
        errors.append("personalization awkwardly inserts a raw repository coordinate")
    if re.search(r"\bi noticed\b[^.]{0,80}(?:@keyframes|data-astro|transform:|min-height:|\[npm-image\])", body, re.I):
        errors.append("personalization contains malformed scraped fragment")
    if re.search(r"\b(repo|project) is active\b", body, re.I) and not re.search(r"\b(updated|commit|release|issue|activity)\b", evidence_text, re.I):
        errors.append("activity claim lacks date or activity evidence")
    if (
        re.search(r"\bwithout building queueing, retries\b|\bfrom scratch\b", body, re.I)
        and CONTROL_PLANE_RE.search(evidence_text)
    ):
        errors.append("draft proposes replacing infrastructure the prospect already owns")
    if re.search(r"\bi noticed you are using gpus?\b", body, re.I) and "gpu" not in " ".join(draft["personalization_claims"]).lower():
        errors.append("GPU claims must be evidence-backed")
    project_terms = [
        term for term in re.split(r"[^a-z0-9]+", draft["project"].lower()) if len(term) >= 3
    ]
    if not any(term in body.lower() for term in project_terms):
        errors.append("body must mention the evidenced project")
    if domains[domain] >= max_per_domain:
        errors.append(f"domain {domain} exceeds the cap of {max_per_domain}")
    return errors


def write_drafts(
    scored: list[dict[str, Any]],
    notes: list[dict[str, Any]],
    use_qwen: bool,
    qwen_ready_override: bool | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool, dict[str, Any]]:
    threshold = int(os.getenv("FIT_SCORE_THRESHOLD", "70"))
    max_per_domain = int(os.getenv("MAX_DRAFTS_PER_DOMAIN", "2"))
    fallback_mode = os.getenv("LLM_FALLBACK_MODE", "template")
    model = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")
    by_id = {note["prospect_id"]: note for note in notes}
    passed: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    domains: Counter[str] = Counter()
    fallback_used = False
    metrics: dict[str, Any] = {
        "requested_model": model if use_qwen else "template",
        "model_invocation_attempted": False,
        "model_invocation_succeeded": False,
        "fallback_reason": "",
        "primary_generated": 0,
        "fallback_generated": 0,
        "retries": 0,
        "latency_ms": 0,
    }
    started = time.monotonic()
    qwen_ready = (
        qwen_ready_override
        if qwen_ready_override is not None
        else use_qwen
        and os.getenv("USE_LOCAL_LLM", "true").lower() == "true"
        and ensure_ollama(model)
    )
    if use_qwen and not qwen_ready:
        if fallback_mode != "template":
            raise RuntimeError("Qwen/Ollama is unavailable and template fallback is disabled.")
        fallback_used = True
        metrics["fallback_reason"] = "qwen_or_ollama_unavailable"
        LOG.warning("Qwen/Ollama unavailable; falling back to template mode.")

    seen_emails: set[str] = set()
    for prospect in scored:
        note = by_id[prospect["prospect_id"]]
        if not prospect.get("email"):
            failures.append(
                {
                    "prospect_id": prospect["prospect_id"],
                    "errors": ["no email contact point; qualified for another channel"],
                }
            )
            continue
        if (
            prospect["fit_score"] < max(threshold, 75)
            or note["evidence_strength"] < 0.8
            or int(prospect.get("contact_quality", 0)) < 7
            or len(prospect.get("evidence_points", [])) < 2
            or prospect.get("excluded")
        ):
            failures.append(
                {
                    "prospect_id": prospect["prospect_id"],
                    "errors": ["prospect did not pass the buyer-fit gate"],
                }
            )
            continue
        if is_generic_contact_email(prospect["email"]):
            failures.append({"prospect_id": prospect["prospect_id"], "errors": ["generic support inbox is not allowed"]})
            continue
        if prospect["email"].lower() in seen_emails:
            failures.append({"prospect_id": prospect["prospect_id"], "errors": ["duplicate email"]})
            continue
        generated = None
        model_mode = "template"
        if qwen_ready:
            try:
                metrics["model_invocation_attempted"] = True
                generated = qwen_draft(prospect, note, model)
                metrics["model_invocation_succeeded"] = generated is not None
                model_mode = "qwen"
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError) as error:
                LOG.warning("Qwen generation failed for %s: %s", prospect["prospect_id"], error)
                if fallback_mode != "template":
                    failures.append(
                        {"prospect_id": prospect["prospect_id"], "errors": ["Qwen generation failed"]}
                    )
                    continue
                fallback_used = True
                metrics["fallback_reason"] = str(error)
                model_mode = "fallback"
        if generated is None:
            if qwen_ready and fallback_mode != "template":
                failures.append(
                    {"prospect_id": prospect["prospect_id"], "errors": ["model returned SKIP"]}
                )
                continue
            generated = template_draft(prospect, note)
            if use_qwen:
                model_mode = "fallback"
                fallback_used = True
                if not metrics["fallback_reason"]:
                    metrics["fallback_reason"] = "model_returned_skip_or_no_output"
        subject, body, claims = generated
        draft = build_draft_candidate(prospect, note, subject, body, claims, model_mode)
        errors = validate_draft(draft, max_per_domain, domains)
        if errors and qwen_ready and model_mode == "qwen" and fallback_mode == "template":
            fallback_used = True
            metrics["fallback_reason"] = "primary_draft_failed_validation"
            subject, body, claims = template_draft(prospect, note)
            draft = build_draft_candidate(prospect, note, subject, body, claims, "fallback")
            errors = validate_draft(draft, max_per_domain, domains)
        if errors:
            failures.append({"prospect_id": prospect["prospect_id"], "errors": errors})
            continue
        domains[prospect["email"].split("@")[-1].lower()] += 1
        seen_emails.add(prospect["email"].lower())
        if draft["model_mode"] == "qwen":
            metrics["primary_generated"] += 1
        elif draft["model_mode"] == "fallback":
            metrics["fallback_generated"] += 1
        passed.append(draft)
    metrics["latency_ms"] = int((time.monotonic() - started) * 1000)
    return passed, failures, fallback_used, metrics


def health_counts(statuses: list[SourceHealth]) -> tuple[list[str], list[str], list[str], list[str]]:
    enabled: list[str] = []
    succeeded: list[str] = []
    degraded: list[str] = []
    failed: list[str] = []
    for status in statuses:
        if status.status != "disabled":
            enabled.append(status.source_type)
        if status.status == "healthy":
            succeeded.append(status.source_type)
        elif status.status == "degraded":
            degraded.append(status.source_type)
        elif status.status not in {"healthy", "disabled"}:
            failed.append(status.source_type)
    return enabled, succeeded, degraded, failed


def write_json(output: Path, name: str, value: Any) -> None:
    output.mkdir(parents=True, exist_ok=True)
    temporary = output / f".{name}.tmp"
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    temporary.replace(output / name)


def run(args: argparse.Namespace) -> int:
    try:
        if args.health_check:
            print("ok")
            return 0
        started = utc_now()
        output = Path(args.output)
        input_path = Path(args.input) if args.input else None
        campaign = configure_campaign(input_path)
        if args.job == "conversation-turn-qwen":
            return run_conversation_turn(output)
        registry = build_default_registry(source_registry_config())
        sources_enabled, sources_succeeded, sources_degraded, sources_failed = health_counts(registry.health())
        prospects, skipped, adapter_signals = discover(args.target, input_path, args.category, registry)
        _, sources_succeeded, sources_degraded, sources_failed = health_counts(registry.health())
        use_qwen = args.job in {"write-emails-qwen", "full-run-qwen"}
        model = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")
        fallback_mode = os.getenv("LLM_FALLBACK_MODE", "template")
        semantic_ready = False
        semantic_degraded = False
        semantic_metrics: dict[str, Any] = {
            "research_attempted": False,
            "research_succeeded": False,
            "qualification_attempted": False,
            "qualification_succeeded": False,
            "scoring_explanation_attempted": False,
            "scoring_explanation_succeeded": False,
            "angle_selection_attempted": False,
            "angle_selection_succeeded": False,
            "validation_attempted": False,
            "validation_succeeded": False,
            "failure_reason": "",
        }
        notes = research(prospects)
        if use_qwen:
            semantic_ready = (
                os.getenv("USE_LOCAL_LLM", "true").lower() == "true" and ensure_ollama(model)
            )
            if not semantic_ready and fallback_mode != "template":
                raise RuntimeError("Qwen/Ollama is unavailable for semantic pipeline stages.")
            if not semantic_ready:
                semantic_degraded = True
                semantic_metrics["failure_reason"] = "qwen_or_ollama_unavailable"
            if semantic_ready and prospects:
                semantic_metrics.update(
                    {
                        "research_attempted": True,
                        "qualification_attempted": True,
                        "scoring_explanation_attempted": True,
                        "angle_selection_attempted": True,
                    }
                )
                try:
                    analysis = qwen_semantic_analysis(prospects, notes, model)
                    prospects, notes, semantic_exclusions = apply_semantic_analysis(
                        prospects, notes, analysis
                    )
                    skipped.extend(semantic_exclusions)
                    semantic_metrics.update(
                        {
                            "research_succeeded": True,
                            "qualification_succeeded": True,
                            "scoring_explanation_succeeded": True,
                            "angle_selection_succeeded": True,
                        }
                    )
                except (
                    urllib.error.URLError,
                    TimeoutError,
                    json.JSONDecodeError,
                    ValueError,
                ) as error:
                    if fallback_mode != "template":
                        raise RuntimeError(f"Semantic analysis failed: {error}") from error
                    semantic_degraded = True
                    semantic_metrics["failure_reason"] = str(error)
        scored = score(prospects, notes)
        drafts: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        fallback_used = False
        model_metrics: dict[str, Any] = {
            "requested_model": "template",
            "model_invocation_attempted": False,
            "model_invocation_succeeded": False,
            "fallback_reason": "",
            "primary_generated": 0,
            "fallback_generated": 0,
            "retries": 0,
            "latency_ms": 0,
        }
        if args.job in {
            "write-emails-template",
            "write-emails-qwen",
            "full-run-template",
            "full-run-qwen",
        }:
            drafts, failures, fallback_used, model_metrics = write_drafts(
                scored,
                notes,
                use_qwen,
                semantic_ready if use_qwen else None,
            )
        qwen_drafts = [draft for draft in drafts if draft.get("model_mode") == "qwen"]
        if use_qwen and semantic_ready and qwen_drafts:
            semantic_metrics["validation_attempted"] = True
            try:
                semantic_validation = qwen_semantic_validate_drafts(qwen_drafts, notes, model)
                drafts, failures = apply_semantic_draft_validation(
                    drafts, failures, semantic_validation
                )
                semantic_metrics["validation_succeeded"] = True
                model_metrics["primary_generated"] = sum(
                    draft.get("model_mode") == "qwen" for draft in drafts
                )
            except (
                urllib.error.URLError,
                TimeoutError,
                json.JSONDecodeError,
                ValueError,
            ) as error:
                if fallback_mode != "template":
                    raise RuntimeError(f"Semantic draft validation failed: {error}") from error
                semantic_degraded = True
                semantic_metrics["failure_reason"] = str(error)
                for draft in qwen_drafts:
                    draft["validation_status"] = "manual_review_required"
                    draft["validation_errors"] = [
                        "semantic model validation was unavailable"
                    ]

        public_prospects = [
            {
                key: value
                for key, value in row.items()
                if key not in {"research_text", "evidence_urls", "stars", "active", "diagnostics"}
                and not (key in {"email", "email_source_url", "email_source_type"} and not value)
            }
            for row in prospects
        ]
        mode = "junglegrid-qwen" if use_qwen else "junglegrid-template"
        fallback_only = use_qwen and fallback_used and model_metrics.get("primary_generated", 0) == 0
        run_status = (
            "degraded"
            if fallback_only or semantic_degraded or sources_failed or sources_degraded
            else "successful"
        )
        canonical_entity_ids = {
            entity["entity_id"]
            for prospect in prospects
            for entity in prospect.get("canonical_entities", [])
            if entity.get("entity_id")
        }
        canonical_relationship_count = sum(len(prospect.get("verified_relationships", [])) for prospect in prospects)
        exclusion_reasons = Counter(
            str(item.get("exclusion_rule_triggered") or item.get("skip_reason") or "unspecified")
            for item in skipped
        )
        semantic_rejection_reasons = Counter(
            str(error)
            for failure in failures
            for error in failure.get("errors", [])
        )
        nonzero_criteria = [
            (row, criterion)
            for row in scored
            for criterion, value in row.get("score_breakdown", {}).items()
            if value > 0
        ]
        criteria_with_evidence = sum(
            bool(row.get("score_evidence_ids", {}).get(criterion))
            for row, criterion in nonzero_criteria
        )
        contamination_candidates = sum(
            item.get("exclusion_rule_triggered") == "contaminated_evidence" for item in skipped
        )
        duplicate_collapse_count = sum(
            item.get("exclusion_rule_triggered") in {"duplicate", "memory_duplicate"}
            or item.get("skip_reason") in {"duplicate", "memory_duplicate"}
            for item in skipped
        )
        generated_count = int(model_metrics["primary_generated"]) + int(
            model_metrics["fallback_generated"]
        )
        quality_metrics = {
            "qualification_gate_pass_rate": round(
                len(prospects) / max(1, len(prospects) + len(skipped)),
                4,
            ),
            "contamination_rejection_rate": 1.0 if contamination_candidates else None,
            "duplicate_collapse_count": duplicate_collapse_count,
            "scored_criteria_with_evidence_ids_percentage": round(
                (criteria_with_evidence / max(1, len(nonzero_criteria))) * 100,
                2,
            ),
            "fallback_rate": round(
                int(model_metrics["fallback_generated"]) / max(1, generated_count),
                4,
            ),
            "semantic_rejection_reasons": dict(semantic_rejection_reasons),
        }
        execution_backend = os.getenv("OUTREACH_EXECUTION_BACKEND", "jungle_grid_mock")
        contract = json.loads(os.getenv("OUTREACH_JOB_CONTRACT", "{}") or "{}")
        summary = {
            "status": run_status,
            "job": args.job,
            "mode": mode,
            "target": args.target,
            "workspace_id": campaign["workspaceId"],
            "campaign_id": campaign["campaignId"],
            "campaign_name": campaign["name"],
            "offer_name": campaign["offer"]["name"],
            "execution_backend": execution_backend,
            "junglegrid_job_id": os.getenv(
                "JUNGLEGRID_JOB_ID", "fixture-job"
            ),
            "production_eligible": execution_backend == "jungle_grid",
            "job_contract_schema_version": contract.get("schema_version", "1.0"),
            "pipeline_stages": contract.get(
                "pipeline_stages",
                [
                    "source_discovery",
                    "prospect_research",
                    "semantic_qualification",
                    "entity_resolution",
                    "prospect_scoring",
                    "outreach_drafting",
                    "semantic_validation",
                ],
            ),
            "score_dimension_labels": {
                "agentMcpRelevance": "campaign target relevance",
                "aiWorkloadRelevance": "campaign workload relevance",
                "infrastructurePain": "documented pain or operating need",
                "openSourceActivity": "recent project activity",
                "jungleGridComprehension": "offer integration compatibility",
                "contactQuality": "contact provenance quality",
            },
            "sources_enabled": sources_enabled,
            "sources_succeeded": sources_succeeded,
            "sources_degraded": sources_degraded,
            "sources_failed": sources_failed,
            "exclusion_reasons": dict(exclusion_reasons),
            "quality_metrics": quality_metrics,
            "source_signals": adapter_signals[:100],
            "discovered_raw": len(prospects) + len(skipped),
            "deduplicated_entities": len(canonical_entity_ids) or len(prospects),
            "canonical_relationships": canonical_relationship_count,
            "qualified": len(prospects),
            "excluded": len(skipped),
            "discovered": len(prospects),
            "researched": len(notes),
            "scored": len(scored),
            "drafted": len(drafts),
            "drafts_passed": len(drafts),
            "drafts_failed": len(failures),
            "skipped": len(skipped) + len(failures),
            "fallback_used": fallback_used,
            "requested_model": model_metrics["requested_model"],
            "model_invocation_attempted": model_metrics["model_invocation_attempted"],
            "model_invocation_succeeded": model_metrics["model_invocation_succeeded"],
            "primary_model_generated": model_metrics["primary_generated"],
            "fallback_generated": model_metrics["fallback_generated"],
            "fallback_reason": model_metrics["fallback_reason"],
            "semantic_stage_metrics": semantic_metrics,
            "model_retries": model_metrics["retries"],
            "model_latency_ms": model_metrics["latency_ms"],
            "model": os.getenv("OLLAMA_MODEL", "qwen2.5:3b") if use_qwen else "template",
            "started_at": started,
            "completed_at": utc_now(),
        }
        status_counts = Counter(draft.get("validation_status", "manual_review_required") for draft in drafts)
        report = {
            "valid": True,
            "checked": len(drafts) + len(failures),
            "passed": int(status_counts.get("send_ready", 0)),
            "failed": len(failures),
            "send_ready": int(status_counts.get("send_ready", 0)),
            "manual_review_required": int(status_counts.get("manual_review_required", 0)),
            "regeneration_required": int(status_counts.get("regeneration_required", 0)),
            "excluded": len(skipped),
            "errors": failures,
            "skipped_prospects": skipped,
        }
        write_json(output, "prospects.json", public_prospects)
        write_json(output, "research_notes.json", notes)
        write_json(output, "scored_prospects.json", scored)
        write_json(output, "email_drafts.json", drafts)
        write_json(output, "run_summary.json", summary)
        write_json(output, "validation_report.json", report)
        LOG.info("Wrote %s validated drafts and %s validation failures.", len(drafts), len(failures))
        return 0
    finally:
        stop_ollama()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", choices=sorted(JOBS), default="full-run-qwen")
    parser.add_argument("--target", type=int, default=17)
    parser.add_argument("--output", default="/workspace/artifacts")
    parser.add_argument("--input")
    parser.add_argument("--category")
    parser.add_argument("--health-check", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))

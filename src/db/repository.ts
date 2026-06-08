import { randomUUID } from "node:crypto";
import type Database from "better-sqlite3";
import { getDatabase } from "@/src/db/database";
import {
  emailDraftSchema,
  prospectSchema,
  researchNoteSchema,
  runSchema,
  settingsSchema,
  suppressionSchema,
  type DraftApprovalStatus,
  type DraftDeliveryStatus,
  type EmailDraft,
  type OutreachRun,
  type OutreachMode,
  type OutreachSettings,
  type Prospect,
  type ProspectCategory,
  type ProspectStatus,
  type ResearchNote,
  type ScoreBreakdown,
  type Suppression,
} from "@/src/domain/schemas";
import { getEnv } from "@/src/config/env";

type ProspectInput = {
  name: string;
  roleTitle?: string | null;
  email: string;
  emailSourceUrl: string;
  emailSourceType: Prospect["emailSourceType"];
  githubUsername?: string | null;
  githubUrl?: string | null;
  websiteUrl?: string | null;
  company?: string | null;
  project: string;
  projectKey: string;
  projectDescription?: string | null;
  category: ProspectCategory;
  confidenceScore?: number | null;
};

type ProspectFilters = {
  search?: string;
  category?: string;
  status?: string;
  minScore?: number;
  source?: string;
  from?: string;
  limit?: number;
};

type DraftInput = {
  subject: string;
  body: string;
  wordCount: number;
  links: string[];
  evidenceUrls: string[];
  personalizationClaims: string[];
  validationStatus: "passed" | "failed";
  validationErrors: string[];
};

type WorkerExclusions = {
  emails: string[];
  domains: string[];
  projectKeys: string[];
};

function now(): string {
  return new Date().toISOString();
}

function parseJson<T>(value: unknown, fallback: T): T {
  if (typeof value !== "string" || value.length === 0) return fallback;
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

function mapProspect(row: Record<string, unknown>): Prospect {
  return prospectSchema.parse({
    id: row.id,
    name: row.name,
    roleTitle: row.role_title,
    email: row.email,
    emailSourceUrl: row.email_source_url,
    emailSourceType: row.email_source_type,
    githubUsername: row.github_username,
    githubUrl: row.github_url,
    websiteUrl: row.website_url,
    company: row.company,
    project: row.project,
    projectKey: row.project_key,
    projectDescription: row.project_description,
    category: row.category,
    fitScore: row.fit_score,
    scoreBreakdown: parseJson<ScoreBreakdown | null>(row.score_breakdown, null),
    confidenceScore: row.confidence_score,
    status: row.status,
    domain: row.domain,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  });
}

function mapResearch(row: Record<string, unknown>): ResearchNote {
  return researchNoteSchema.parse({
    id: row.id,
    prospectId: row.prospect_id,
    summary: row.summary,
    personalizationDetail: row.personalization_detail,
    junglegridRelevance: row.junglegrid_relevance,
    evidenceUrls: parseJson<string[]>(row.evidence_urls, []),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  });
}

function mapDraft(row: Record<string, unknown>): EmailDraft {
  return emailDraftSchema.parse({
    id: row.id,
    prospectId: row.prospect_id,
    toEmail: row.to_email,
    fromEmail: row.from_email,
    fromName: row.from_name,
    replyTo: row.reply_to,
    subject: row.subject,
    body: row.body,
    wordCount: row.word_count,
    links: parseJson<string[]>(row.links, []),
    evidenceUrls: parseJson<string[]>(row.evidence_urls, []),
    personalizationClaims: parseJson<string[]>(row.personalization_claims, []),
    validationStatus: row.validation_status,
    validationErrors: parseJson<string[]>(row.validation_errors, []),
    approvalStatus: row.approval_status,
    deliveryStatus: row.delivery_status,
    approvedAt: row.approved_at,
    approvedBy: row.approved_by,
    sentAt: row.sent_at,
    zeptomailMessageId: row.zeptomail_message_id,
    zeptomailRequestId: row.zeptomail_request_id,
    zeptomailError: row.zeptomail_error,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  });
}

function mapSuppression(row: Record<string, unknown>): Suppression {
  return suppressionSchema.parse({
    id: row.id,
    email: row.email,
    domain: row.domain,
    reason: row.reason,
    source: row.source,
    createdAt: row.created_at,
  });
}

function mapRun(row: Record<string, unknown>): OutreachRun {
  return runSchema.parse({
    id: row.id,
    runType: row.run_type,
    mode: row.mode ?? "local-template",
    junglegridJobId: row.junglegrid_job_id ?? null,
    targetCount: row.target_count,
    draftedCount: row.drafted_count,
    failedCount: row.failed_count,
    retryCount: row.retry_count ?? 0,
    modelMode: row.model_mode ?? null,
    artifacts: parseJson<string[]>(row.artifacts_json, []),
    phase: row.phase,
    notes: row.notes,
    error: row.error,
    createdAt: row.created_at,
    startedAt: row.started_at,
    completedAt: row.completed_at,
  });
}

export class OutreachRepository {
  constructor(private readonly db: Database.Database = getDatabase()) {}

  upsertProspect(input: ProspectInput): { prospect: Prospect; created: boolean } {
    const normalizedEmail = input.email.trim().toLowerCase();
    const domain = normalizedEmail.split("@")[1];
    const existing = this.db
      .prepare(
        `SELECT * FROM prospects
         WHERE normalized_email = ?
            OR (? IS NOT NULL AND lower(github_username) = lower(?))
            OR project_key = ?
         LIMIT 1`,
      )
      .get(normalizedEmail, input.githubUsername ?? null, input.githubUsername ?? null, input.projectKey) as
      | Record<string, unknown>
      | undefined;

    if (existing) return { prospect: mapProspect(existing), created: false };

    const id = randomUUID();
    const timestamp = now();
    this.db
      .prepare(
        `INSERT INTO prospects (
          id, name, role_title, email, normalized_email, email_source_url, email_source_type,
          github_username, github_url, website_url, company, project, project_key,
          project_description, category, confidence_score, status, domain, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'found', ?, ?, ?)`,
      )
      .run(
        id,
        input.name,
        input.roleTitle ?? null,
        input.email,
        normalizedEmail,
        input.emailSourceUrl,
        input.emailSourceType,
        input.githubUsername ?? null,
        input.githubUrl ?? null,
        input.websiteUrl ?? null,
        input.company ?? null,
        input.project,
        input.projectKey,
        input.projectDescription ?? null,
        input.category,
        input.confidenceScore ?? null,
        domain,
        timestamp,
        timestamp,
      );
    this.audit("system", "prospect.created", "prospect", id, "success", {
      source: input.emailSourceType,
    });
    return { prospect: this.getProspect(id)!, created: true };
  }

  listProspects(filters: ProspectFilters = {}): Prospect[] {
    const clauses: string[] = [];
    const params: unknown[] = [];
    if (filters.search) {
      clauses.push(
        `(lower(name) LIKE ? OR lower(email) LIKE ? OR lower(domain) LIKE ? OR
          lower(COALESCE(company, '')) LIKE ? OR lower(project) LIKE ? OR
          lower(COALESCE(github_username, '')) LIKE ?)`,
      );
      const term = `%${filters.search.toLowerCase()}%`;
      params.push(term, term, term, term, term, term);
    }
    if (filters.category) {
      clauses.push("category = ?");
      params.push(filters.category);
    }
    if (filters.status) {
      clauses.push("status = ?");
      params.push(filters.status);
    }
    if (filters.source) {
      clauses.push("email_source_type = ?");
      params.push(filters.source);
    }
    if (filters.minScore !== undefined) {
      clauses.push("fit_score >= ?");
      params.push(filters.minScore);
    }
    if (filters.from) {
      clauses.push("created_at >= ?");
      params.push(filters.from);
    }
    params.push(Math.min(filters.limit ?? 200, 1000));
    const rows = this.db
      .prepare(
        `SELECT * FROM prospects ${clauses.length ? `WHERE ${clauses.join(" AND ")}` : ""}
         ORDER BY COALESCE(fit_score, -1) DESC, created_at DESC LIMIT ?`,
      )
      .all(...params) as Record<string, unknown>[];
    return rows.map(mapProspect);
  }

  getProspect(id: string): Prospect | null {
    const row = this.db.prepare("SELECT * FROM prospects WHERE id = ?").get(id) as
      | Record<string, unknown>
      | undefined;
    return row ? mapProspect(row) : null;
  }

  updateProspect(
    id: string,
    patch: Partial<Pick<Prospect, "name" | "roleTitle" | "company" | "category" | "status">>,
  ): Prospect {
    const current = this.getProspect(id);
    if (!current) throw new Error("Prospect not found.");
    const next = { ...current, ...patch, updatedAt: now() };
    this.db
      .prepare(
        `UPDATE prospects SET name = ?, role_title = ?, company = ?, category = ?, status = ?,
         updated_at = ? WHERE id = ?`,
      )
      .run(
        next.name,
        next.roleTitle,
        next.company,
        next.category,
        next.status,
        next.updatedAt,
        id,
      );
    this.audit("operator", "prospect.updated", "prospect", id, "success", patch);
    return this.getProspect(id)!;
  }

  setProspectStatus(id: string, status: ProspectStatus): void {
    this.db
      .prepare("UPDATE prospects SET status = ?, updated_at = ? WHERE id = ?")
      .run(status, now(), id);
  }

  setScore(id: string, fitScore: number, breakdown: ScoreBreakdown): void {
    this.db
      .prepare(
        `UPDATE prospects SET fit_score = ?, score_breakdown = ?, status = 'scored',
         updated_at = ? WHERE id = ?`,
      )
      .run(fitScore, JSON.stringify(breakdown), now(), id);
  }

  saveResearch(
    prospectId: string,
    input: Omit<ResearchNote, "id" | "prospectId" | "createdAt" | "updatedAt">,
  ): ResearchNote {
    const existing = this.getResearch(prospectId);
    const timestamp = now();
    if (existing) {
      this.db
        .prepare(
          `UPDATE research_notes SET summary = ?, personalization_detail = ?,
           junglegrid_relevance = ?, evidence_urls = ?, updated_at = ? WHERE prospect_id = ?`,
        )
        .run(
          input.summary,
          input.personalizationDetail,
          input.junglegridRelevance,
          JSON.stringify(input.evidenceUrls),
          timestamp,
          prospectId,
        );
    } else {
      this.db
        .prepare(
          `INSERT INTO research_notes (
            id, prospect_id, summary, personalization_detail, junglegrid_relevance,
            evidence_urls, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          randomUUID(),
          prospectId,
          input.summary,
          input.personalizationDetail,
          input.junglegridRelevance,
          JSON.stringify(input.evidenceUrls),
          timestamp,
          timestamp,
        );
    }
    this.setProspectStatus(prospectId, "researched");
    return this.getResearch(prospectId)!;
  }

  getResearch(prospectId: string): ResearchNote | null {
    const row = this.db
      .prepare("SELECT * FROM research_notes WHERE prospect_id = ?")
      .get(prospectId) as Record<string, unknown> | undefined;
    return row ? mapResearch(row) : null;
  }

  saveDraft(prospectId: string, input: DraftInput): EmailDraft {
    const existing = this.getDraftByProspect(prospectId);
    const prospect = this.getProspect(prospectId);
    if (!prospect) throw new Error("Prospect not found.");
    const env = getEnv();
    const timestamp = now();
    if (existing) {
      this.db
        .prepare(
          `UPDATE email_drafts SET to_email = ?, from_email = ?, from_name = ?, reply_to = ?,
           subject = ?, body = ?, word_count = ?, links = ?, evidence_urls = ?,
           personalization_claims = ?, validation_status = ?, validation_errors = ?,
           approval_status = CASE WHEN delivery_status IN ('not_sent', 'failed') THEN 'pending_review' ELSE approval_status END,
           delivery_status = CASE WHEN delivery_status = 'failed' THEN 'not_sent' ELSE delivery_status END,
           zeptomail_error = CASE WHEN delivery_status = 'failed' THEN NULL ELSE zeptomail_error END,
           updated_at = ? WHERE prospect_id = ?`,
        )
        .run(
          prospect.email,
          env.ZEPTOMAIL_FROM_EMAIL,
          env.ZEPTOMAIL_FROM_NAME,
          env.ZEPTOMAIL_REPLY_TO,
          input.subject,
          input.body,
          input.wordCount,
          JSON.stringify(input.links),
          JSON.stringify(input.evidenceUrls),
          JSON.stringify(input.personalizationClaims),
          input.validationStatus,
          JSON.stringify(input.validationErrors),
          timestamp,
          prospectId,
        );
    } else {
      this.db
        .prepare(
          `INSERT INTO email_drafts (
            id, prospect_id, to_email, from_email, from_name, reply_to, subject, body,
            word_count, links, evidence_urls, personalization_claims, validation_status,
            validation_errors, approval_status, delivery_status, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending_review', 'not_sent', ?, ?)`,
        )
        .run(
          randomUUID(),
          prospectId,
          prospect.email,
          env.ZEPTOMAIL_FROM_EMAIL,
          env.ZEPTOMAIL_FROM_NAME,
          env.ZEPTOMAIL_REPLY_TO,
          input.subject,
          input.body,
          input.wordCount,
          JSON.stringify(input.links),
          JSON.stringify(input.evidenceUrls),
          JSON.stringify(input.personalizationClaims),
          input.validationStatus,
          JSON.stringify(input.validationErrors),
          timestamp,
          timestamp,
        );
    }
    this.setProspectStatus(prospectId, "drafted");
    return this.getDraftByProspect(prospectId)!;
  }

  listDrafts(): Array<EmailDraft & { prospect: Prospect }> {
    const rows = this.db
      .prepare(
        `SELECT d.*, p.id AS p_id, p.name AS p_name, p.role_title AS p_role_title,
         p.email AS p_email, p.email_source_url AS p_email_source_url,
         p.email_source_type AS p_email_source_type, p.github_username AS p_github_username,
         p.github_url AS p_github_url, p.website_url AS p_website_url, p.company AS p_company,
         p.project AS p_project, p.project_key AS p_project_key,
         p.project_description AS p_project_description, p.category AS p_category,
         p.fit_score AS p_fit_score, p.score_breakdown AS p_score_breakdown,
         p.confidence_score AS p_confidence_score, p.status AS p_status, p.domain AS p_domain,
         p.created_at AS p_created_at, p.updated_at AS p_updated_at
         FROM email_drafts d JOIN prospects p ON p.id = d.prospect_id
         ORDER BY d.created_at DESC`,
      )
      .all() as Record<string, unknown>[];

    return rows.map((row) => ({
      ...mapDraft(row),
      prospect: mapProspect({
        id: row.p_id,
        name: row.p_name,
        role_title: row.p_role_title,
        email: row.p_email,
        email_source_url: row.p_email_source_url,
        email_source_type: row.p_email_source_type,
        github_username: row.p_github_username,
        github_url: row.p_github_url,
        website_url: row.p_website_url,
        company: row.p_company,
        project: row.p_project,
        project_key: row.p_project_key,
        project_description: row.p_project_description,
        category: row.p_category,
        fit_score: row.p_fit_score,
        score_breakdown: row.p_score_breakdown,
        confidence_score: row.p_confidence_score,
        status: row.p_status,
        domain: row.p_domain,
        created_at: row.p_created_at,
        updated_at: row.p_updated_at,
      }),
    }));
  }

  getDraft(id: string): EmailDraft | null {
    const row = this.db.prepare("SELECT * FROM email_drafts WHERE id = ?").get(id) as
      | Record<string, unknown>
      | undefined;
    return row ? mapDraft(row) : null;
  }

  getDraftByProspect(prospectId: string): EmailDraft | null {
    const row = this.db
      .prepare("SELECT * FROM email_drafts WHERE prospect_id = ?")
      .get(prospectId) as Record<string, unknown> | undefined;
    return row ? mapDraft(row) : null;
  }

  setDraftApproval(
    draftId: string,
    approvalStatus: DraftApprovalStatus,
    approvedBy?: string | null,
  ): EmailDraft {
    const draft = this.getDraft(draftId);
    if (!draft) throw new Error("Draft not found.");
    const approvedAt = approvalStatus === "approved" ? now() : null;
    this.db
      .prepare(
        `UPDATE email_drafts SET approval_status = ?, approved_at = ?, approved_by = ?,
         updated_at = ? WHERE id = ?`,
      )
      .run(approvalStatus, approvedAt, approvalStatus === "approved" ? approvedBy ?? "operator" : null, now(), draftId);
    const prospectStatus =
      approvalStatus === "approved"
        ? "reviewed"
        : approvalStatus === "rejected"
          ? "rejected"
          : "drafted";
    this.setProspectStatus(draft.prospectId, prospectStatus);
    this.audit("operator", `draft.${approvalStatus}`, "email_draft", draftId, "success");
    return this.getDraft(draftId)!;
  }

  setDraftDelivery(
    draftId: string,
    deliveryStatus: DraftDeliveryStatus,
    input: {
      zeptomailMessageId?: string | null;
      zeptomailRequestId?: string | null;
      zeptomailError?: string | null;
    } = {},
  ): EmailDraft {
    const draft = this.getDraft(draftId);
    if (!draft) throw new Error("Draft not found.");
    const timestamp = now();
    this.db
      .prepare(
        `UPDATE email_drafts SET delivery_status = ?, sent_at = CASE WHEN ? = 'sent' THEN ? ELSE sent_at END,
         zeptomail_message_id = COALESCE(?, zeptomail_message_id),
         zeptomail_request_id = COALESCE(?, zeptomail_request_id),
         zeptomail_error = ?, updated_at = ? WHERE id = ?`,
      )
      .run(
        deliveryStatus,
        deliveryStatus,
        timestamp,
        input.zeptomailMessageId ?? null,
        input.zeptomailRequestId ?? null,
        input.zeptomailError ?? null,
        timestamp,
        draftId,
      );
    const prospectStatus =
      deliveryStatus === "sent"
        ? "sent_manually"
        : deliveryStatus === "replied"
          ? "replied"
          : deliveryStatus === "bounced"
            ? "bounced"
            : deliveryStatus === "failed"
              ? "reviewed"
              : "drafted";
    this.setProspectStatus(draft.prospectId, prospectStatus);
    return this.getDraft(draftId)!;
  }

  setDraftReviewStatus(id: string, status: "replied" | "bounced"): EmailDraft {
    return this.setDraftDelivery(id, status);
  }

  markDraftRejected(id: string): EmailDraft {
    return this.setDraftApproval(id, "rejected", null);
  }

  setDraftStatus(id: string, status: "reviewed" | "sent_manually" | "replied" | "bounced"): EmailDraft {
    if (status === "reviewed") return this.setDraftApproval(id, "approved", "operator");
    if (status === "sent_manually") return this.setDraftDelivery(id, "sent");
    if (status === "replied" || status === "bounced") return this.setDraftReviewStatus(id, status);
    const draft = this.getDraft(id);
    if (!draft) throw new Error("Draft not found.");
    return draft;
  }

  countApprovedDrafts(): number {
    const row = this.db
      .prepare("SELECT COUNT(*) AS count FROM email_drafts WHERE approval_status = 'approved'")
      .get() as { count: number };
    return row.count;
  }

  countDeliveryStatus(status: DraftDeliveryStatus): number {
    const row = this.db
      .prepare("SELECT COUNT(*) AS count FROM email_drafts WHERE delivery_status = ?")
      .get(status) as { count: number };
    return row.count;
  }

  createRun(
    runType: string,
    targetCount: number,
    notes?: string,
    mode: OutreachMode = getEnv().JUNGLEGRID_MODE,
  ): OutreachRun {
    const id = randomUUID();
    this.db
      .prepare(
        `INSERT INTO outreach_runs (
          id, run_type, mode, target_count, drafted_count, failed_count, retry_count,
          artifacts_json, phase, notes, created_at
        ) VALUES (?, ?, ?, ?, 0, 0, 0, '[]', 'queued', ?, ?)`,
      )
      .run(id, runType, mode, targetCount, notes ?? null, now());
    return this.getRun(id)!;
  }

  updateRun(
    id: string,
    patch: Partial<
      Pick<
        OutreachRun,
        | "phase"
        | "draftedCount"
        | "failedCount"
        | "notes"
        | "error"
        | "junglegridJobId"
        | "retryCount"
        | "modelMode"
        | "artifacts"
      >
    >,
  ): OutreachRun {
    const run = this.getRun(id);
    if (!run) throw new Error("Run not found.");
    const next = { ...run, ...patch };
    const startedAt = run.startedAt ?? (next.phase !== "queued" ? now() : null);
    const completedAt =
      next.phase === "completed" || next.phase === "failed" ? run.completedAt ?? now() : null;
    this.db
      .prepare(
        `UPDATE outreach_runs SET phase = ?, drafted_count = ?, failed_count = ?, notes = ?,
         error = ?, junglegrid_job_id = ?, retry_count = ?, model_mode = ?,
         artifacts_json = ?, started_at = ?, completed_at = ? WHERE id = ?`,
      )
      .run(
        next.phase,
        next.draftedCount,
        next.failedCount,
        next.notes,
        next.error,
        next.junglegridJobId,
        next.retryCount,
        next.modelMode,
        JSON.stringify(next.artifacts),
        startedAt,
        completedAt,
        id,
      );
    return this.getRun(id)!;
  }

  addRunEvent(
    runId: string,
    phase: string,
    message: string,
    level = "info",
    metadata?: unknown,
  ): void {
    this.db
      .prepare(
        `INSERT INTO run_events (run_id, phase, level, message, metadata, created_at)
         VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run(runId, phase, level, message, metadata ? JSON.stringify(metadata) : null, now());
  }

  linkRunProspect(runId: string, prospectId: string, outcome: string, reason?: string): void {
    this.db
      .prepare(
        `INSERT INTO run_prospects (run_id, prospect_id, outcome, reason) VALUES (?, ?, ?, ?)
         ON CONFLICT(run_id, prospect_id) DO UPDATE SET outcome = excluded.outcome,
         reason = excluded.reason`,
      )
      .run(runId, prospectId, outcome, reason ?? null);
  }

  getRun(id: string): OutreachRun | null {
    const row = this.db.prepare("SELECT * FROM outreach_runs WHERE id = ?").get(id) as
      | Record<string, unknown>
      | undefined;
    return row ? mapRun(row) : null;
  }

  listRuns(limit = 100): OutreachRun[] {
    return (
      this.db
        .prepare("SELECT * FROM outreach_runs ORDER BY created_at DESC LIMIT ?")
        .all(limit) as Record<string, unknown>[]
    ).map(mapRun);
  }

  getRunDetail(id: string): { run: OutreachRun; events: unknown[]; prospects: unknown[] } | null {
    const run = this.getRun(id);
    if (!run) return null;
    const events = this.db
      .prepare("SELECT * FROM run_events WHERE run_id = ? ORDER BY id ASC")
      .all(id) as Record<string, unknown>[];
    const prospects = this.db
      .prepare(
        `SELECT rp.outcome, rp.reason, p.* FROM run_prospects rp
         JOIN prospects p ON p.id = rp.prospect_id WHERE rp.run_id = ?
         ORDER BY p.fit_score DESC`,
      )
      .all(id) as Record<string, unknown>[];
    return {
      run,
      events: events.map((event) => ({
        ...event,
        metadata: parseJson(event.metadata, null),
      })),
      prospects: prospects.map((row) => ({
        outcome: row.outcome,
        reason: row.reason,
        prospect: mapProspect(row),
      })),
    };
  }

  blockContact(prospectId: string, reason: string): void {
    const prospect = this.getProspect(prospectId);
    if (!prospect) throw new Error("Prospect not found.");
    this.addBlockedContact({ email: prospect.email, reason });
    this.setProspectStatus(prospectId, "blocked");
    this.audit("operator", "contact.blocked", "prospect", prospectId, "success", { reason });
  }

  addBlockedContact(input: { email?: string; domain?: string; reason: string }): void {
    const email = input.email?.trim().toLowerCase() || null;
    const domain = input.domain?.trim().toLowerCase().replace(/^@/, "") || null;
    if (!email && !domain) throw new Error("A blocklist email or domain is required.");
    const existing = this.db
      .prepare(
        `SELECT id FROM blocked_contacts
         WHERE (? IS NOT NULL AND lower(email) = lower(?))
            OR (? IS NOT NULL AND lower(domain) = lower(?)) LIMIT 1`,
      )
      .get(email, email, domain, domain) as { id: string } | undefined;
    if (existing) {
      this.db
        .prepare("UPDATE blocked_contacts SET reason = ? WHERE id = ?")
        .run(input.reason, existing.id);
      return;
    }
    this.db
      .prepare(
        "INSERT INTO blocked_contacts (id, email, domain, reason, created_at) VALUES (?, ?, ?, ?, ?)",
      )
      .run(randomUUID(), email, domain, input.reason, now());
  }

  removeBlockedContact(id: string): void {
    this.db.prepare("DELETE FROM blocked_contacts WHERE id = ?").run(id);
    this.audit("operator", "blocklist.removed", "blocked_contact", id, "success");
  }

  isBlocked(email: string, domain: string): boolean {
    const row = this.db
      .prepare(
        `SELECT id FROM blocked_contacts
         WHERE lower(email) = lower(?) OR lower(domain) = lower(?) LIMIT 1`,
      )
      .get(email, domain);
    return Boolean(row);
  }

  listBlocked(): Array<Record<string, unknown>> {
    return this.db
      .prepare("SELECT * FROM blocked_contacts ORDER BY created_at DESC")
      .all() as Array<Record<string, unknown>>;
  }

  addSuppression(input: {
    email?: string | null;
    domain?: string | null;
    reason: string;
    source?: string;
  }): Suppression {
    const email = input.email?.trim().toLowerCase() || null;
    const domain = input.domain?.trim().toLowerCase().replace(/^@/, "") || null;
    if (!email && !domain) throw new Error("A suppression email or domain is required.");
    const existing = this.db
      .prepare(
        `SELECT id FROM suppressions
         WHERE (? IS NOT NULL AND lower(email) = lower(?))
            OR (? IS NOT NULL AND lower(domain) = lower(?)) LIMIT 1`,
      )
      .get(email, email, domain, domain) as { id: string } | undefined;
    if (existing) {
      this.db
        .prepare("UPDATE suppressions SET reason = ?, source = ? WHERE id = ?")
        .run(input.reason, input.source ?? "operator", existing.id);
      return this.getSuppression(existing.id)!;
    }
    const id = randomUUID();
    this.db
      .prepare(
        `INSERT INTO suppressions (id, email, domain, reason, source, created_at)
         VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run(id, email, domain, input.reason, input.source ?? "operator", now());
    this.audit("operator", "suppression.created", "suppression", id, "success", {
      email,
      domain,
    });
    return this.getSuppression(id)!;
  }

  getSuppression(id: string): Suppression | null {
    const row = this.db.prepare("SELECT * FROM suppressions WHERE id = ?").get(id) as
      | Record<string, unknown>
      | undefined;
    return row ? mapSuppression(row) : null;
  }

  listSuppressions(): Suppression[] {
    return (
      this.db
        .prepare("SELECT * FROM suppressions ORDER BY created_at DESC")
        .all() as Record<string, unknown>[]
    ).map(mapSuppression);
  }

  isSuppressed(email: string, domain: string): boolean {
    const row = this.db
      .prepare(
        `SELECT id FROM suppressions
         WHERE lower(email) = lower(?) OR lower(domain) = lower(?) LIMIT 1`,
      )
      .get(email, domain);
    return Boolean(row);
  }

  getWorkerExclusions(): WorkerExclusions {
    const prospectRows = this.db
      .prepare("SELECT normalized_email, domain, project_key FROM prospects")
      .all() as Array<{ normalized_email: string | null; domain: string | null; project_key: string | null }>;
    const blockedRows = this.db
      .prepare("SELECT email, domain FROM blocked_contacts")
      .all() as Array<{ email: string | null; domain: string | null }>;
    const suppressionRows = this.db
      .prepare("SELECT email, domain FROM suppressions")
      .all() as Array<{ email: string | null; domain: string | null }>;

    const emails = new Set<string>();
    const domains = new Set<string>();
    const projectKeys = new Set<string>();

    for (const row of prospectRows) {
      if (row.normalized_email) emails.add(row.normalized_email.toLowerCase());
      if (row.domain) domains.add(row.domain.toLowerCase());
      if (row.project_key) projectKeys.add(row.project_key.toLowerCase());
    }
    for (const row of [...blockedRows, ...suppressionRows]) {
      if (row.email) emails.add(row.email.toLowerCase());
      if (row.domain) domains.add(row.domain.toLowerCase());
    }

    return {
      emails: [...emails].sort(),
      domains: [...domains].sort(),
      projectKeys: [...projectKeys].sort(),
    };
  }

  countDraftsSince(isoDate: string): number {
    const row = this.db
      .prepare(
        `SELECT COUNT(*) AS count FROM email_drafts
         WHERE delivery_status = 'sent' AND sent_at >= ?`,
      )
      .get(isoDate) as { count: number };
    return row.count;
  }

  countDomainDraftsSince(domain: string, isoDate: string): number {
    const row = this.db
      .prepare(
        `SELECT COUNT(*) AS count FROM email_drafts d
         JOIN prospects p ON p.id = d.prospect_id
         WHERE d.delivery_status = 'sent' AND p.domain = ? AND d.sent_at >= ?`,
      )
      .get(domain, isoDate) as { count: number };
    return row.count;
  }

  getSettings(): OutreachSettings {
    const env = getEnv();
    const defaults: OutreachSettings = {
      dailyTarget: env.DAILY_TARGET,
      fitScoreThreshold: env.FIT_SCORE_THRESHOLD,
      perDomainCap: env.MAX_DRAFTS_PER_DOMAIN,
      mode: env.JUNGLEGRID_MODE,
      modelName: env.OLLAMA_MODEL,
      workerImage: env.JUNGLEGRID_DEFAULT_IMAGE,
      dryRun: env.DRY_RUN,
      junglegridSite: env.JUNGLEGRID_SITE,
    };
    const rows = this.db.prepare("SELECT key, value FROM settings").all() as Array<{
      key: string;
      value: string;
    }>;
    const stored = Object.fromEntries(rows.map((row) => [row.key, JSON.parse(row.value)]));
    return settingsSchema.parse({ ...defaults, ...stored });
  }

  saveSettings(input: OutreachSettings): OutreachSettings {
    const settings = settingsSchema.parse(input);
    const statement = this.db.prepare(
      `INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
       ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
    );
    const transaction = this.db.transaction(() => {
      for (const [key, value] of Object.entries(settings)) {
        statement.run(key, JSON.stringify(value), now());
      }
    });
    transaction();
    this.audit("operator", "settings.updated", "settings", null, "success");
    return settings;
  }

  dashboardSummary(): Record<string, unknown> {
    const statusRows = this.db
      .prepare("SELECT status, COUNT(*) AS count FROM prospects GROUP BY status")
      .all() as Array<{ status: string; count: number }>;
    const counts = Object.fromEntries(statusRows.map((row) => [row.status, row.count]));
    const settings = this.getSettings();
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    return {
      counts,
      totalProspects: Object.values(counts).reduce((sum, value) => sum + Number(value), 0),
      todayDrafted: this.countDraftsSince(today.toISOString()),
      dailyTarget: settings.dailyTarget,
      approvedDrafts: this.countApprovedDrafts(),
      sentDrafts: this.countDeliveryStatus("sent"),
      failedSends: this.countDeliveryStatus("failed"),
      blockedContacts: this.listBlocked().length + this.listSuppressions().length,
      latestRun: this.listRuns(1)[0] ?? null,
    };
  }

  audit(
    actor: string,
    action: string,
    entityType: string,
    entityId: string | null,
    outcome: string,
    metadata?: unknown,
  ): void {
    this.db
      .prepare(
        `INSERT INTO audit_logs (
          actor, action, entity_type, entity_id, outcome, metadata, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        actor,
        action,
        entityType,
        entityId,
        outcome,
        metadata ? JSON.stringify(metadata) : null,
        now(),
      );
  }
}

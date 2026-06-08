import type { OutreachMode, ProspectCategory } from "@/src/domain/schemas";
import { JungleGridWorkloadProvider } from "@/src/providers/junglegrid-workload-provider";
import { artifactNames, ingestArtifactBundle } from "@/src/services/artifact-ingestion";
import { OutreachService } from "@/src/services/outreach-service";

export type RunOptions = {
  targetCount: number;
  discoveryLimit?: number;
  category?: ProspectCategory;
  scoreThreshold?: number;
  dryRun?: boolean;
  mode?: OutreachMode;
};

export async function runOutreach(
  options: RunOptions,
  service = new OutreachService(),
  existingRunId?: string,
): Promise<{ runId: string; summary: Record<string, unknown> }> {
  const repository = service.repository;
  const mode = options.mode ?? repository.getSettings().mode;
  const run = existingRunId
    ? repository.getRun(existingRunId)
    : repository.createRun("full", options.targetCount, JSON.stringify(options), mode);
  if (!run) throw new Error("Run not found.");
  if (mode !== "local-template") {
    return runJungleGridOutreach(run.id, { ...options, mode }, service);
  }
  let failed = 0;
  let drafted = 0;

  try {
    repository.updateRun(run.id, { phase: "discovering" });
    repository.addRunEvent(run.id, "discovering", "Searching public GitHub projects.");
    const discovery = await service.discover(
      options.discoveryLimit ?? Math.max(50, options.targetCount * 3),
      options.category,
    );
    repository.addRunEvent(run.id, "discovering", "Discovery completed.", "info", discovery);

    repository.updateRun(run.id, { phase: "researching" });
    repository.addRunEvent(run.id, "researching", "Collecting public project evidence.");
    const research = await service.research(Math.max(30, options.targetCount * 2));
    failed += research.failed;
    repository.addRunEvent(run.id, "researching", "Research completed.", "info", research);

    repository.updateRun(run.id, { phase: "scoring", failedCount: failed });
    const scoring = service.score();
    repository.addRunEvent(run.id, "scoring", "Fit scoring completed.", "info", scoring);

    repository.updateRun(run.id, { phase: "writing" });
    repository.addRunEvent(
      run.id,
      "writing",
      "Drafting only operator-approved prospects that meet all evidence and score gates.",
    );
    const draftResult = await service.draftApproved(options.targetCount, {
      dryRun: options.dryRun,
      scoreThreshold: options.scoreThreshold,
    });
    drafted = draftResult.drafted;
    failed += draftResult.failed;
    repository.addRunEvent(run.id, "writing", "Draft stage completed.", "info", draftResult);

    repository.updateRun(run.id, {
      phase: "completed",
      draftedCount: drafted,
      failedCount: failed,
      notes:
        drafted === 0
          ? "No approved prospects were eligible. Review scored prospects and approve them before drafting."
          : null,
    });
    return {
      runId: run.id,
      summary: { discovery, research, scoring, ...draftResult },
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown run failure";
    repository.addRunEvent(run.id, "failed", message, "error");
    repository.updateRun(run.id, {
      phase: "failed",
      draftedCount: drafted,
      failedCount: failed + 1,
      error: message,
    });
    throw error;
  }
}

async function runJungleGridOutreach(
  runId: string,
  options: RunOptions & { mode: Exclude<OutreachMode, "local-template"> },
  service: OutreachService,
): Promise<{ runId: string; summary: Record<string, unknown> }> {
  const repository = service.repository;
  const provider = new JungleGridWorkloadProvider();
  const exclusions = repository.getWorkerExclusions();
  try {
    if (!provider.available()) throw new Error("JUNGLEGRID_API_KEY is not configured.");
    repository.updateRun(runId, { phase: "discovering" });
    repository.addRunEvent(
      runId,
      "discovering",
      `Submitting ${options.mode} worker to Jungle Grid.`,
    );
    const job = await provider.submit(options.mode, options.targetCount, options.category, exclusions);
    repository.updateRun(runId, { junglegridJobId: job.job_id });
    repository.addRunEvent(runId, "discovering", "Jungle Grid job submitted.", "info", {
      jobId: job.job_id,
      status: job.status,
      mode: options.mode,
    });

    const completed = await provider.waitForCompletion(job.job_id, (status) => {
      repository.addRunEvent(
        runId,
        status.execution_phase ?? status.status,
        `Jungle Grid job status: ${status.status}.`,
        "info",
        {
          status: status.status,
          executionPhase: status.execution_phase,
          delayedStart: status.delayed_start,
        },
      );
    });
    if (completed.status !== "completed") {
      throw new Error(
        `Jungle Grid job ended with status ${completed.status}: ${
          completed.status_reason ?? "No status reason provided."
        }`,
      );
    }

    repository.updateRun(runId, { phase: "downloading_artifacts" });
    const bundle = await provider.downloadArtifactBundle(job.job_id);
    repository.updateRun(runId, { phase: "validating" });
    const ingestion = ingestArtifactBundle(bundle, repository);
    repository.updateRun(runId, {
      phase: "completed",
      draftedCount: ingestion.drafted,
      failedCount: ingestion.failed,
      modelMode: ingestion.modelMode,
      artifacts: artifactNames(),
      notes:
        ingestion.drafted === 0
          ? "The worker completed, but no prospects passed all evidence and validation gates."
          : null,
    });
    repository.addRunEvent(runId, "completed", "Artifacts validated and local drafts persisted.", "info", {
      ...ingestion,
      artifacts: artifactNames(),
    });
    return {
      runId,
      summary: {
        junglegridJobId: job.job_id,
        mode: options.mode,
        ...ingestion,
        runSummary: bundle.run_summary,
      },
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown Jungle Grid run failure.";
    const run = repository.getRun(runId);
    repository.addRunEvent(runId, "failed", message, "error");
    repository.updateRun(runId, {
      phase: "failed",
      failedCount: (run?.failedCount ?? 0) + 1,
      error: message,
    });
    throw error;
  }
}

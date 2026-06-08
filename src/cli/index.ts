#!/usr/bin/env node
import { Command } from "commander";
import fs from "node:fs";
import path from "node:path";
import { OutreachService } from "@/src/services/outreach-service";
import { runOutreach } from "@/src/services/run-orchestrator";
import {
  outreachModeSchema,
  prospectCategorySchema,
  type OutreachMode,
} from "@/src/domain/schemas";
import { JungleGridWorkloadProvider } from "@/src/providers/junglegrid-workload-provider";
import {
  getZeptoMailStatus,
  ZeptoMailService,
} from "@/apps/api/src/services/zeptomail";

const program = new Command();
program
  .name("jungle-outreach")
  .description("Public-evidence, draft-only outreach workflow powered by Jungle Grid.")
  .showHelpAfterError();

function positiveInteger(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < 1) throw new Error("Expected a positive integer.");
  return parsed;
}

function getCategory(value?: string) {
  return value ? prospectCategorySchema.parse(value) : undefined;
}

async function executeRun(
  mode: OutreachMode,
  options: { count: number; category?: string; dryRun?: boolean },
) {
  const result = await runOutreach({
    targetCount: options.count,
    category: getCategory(options.category),
    dryRun: options.dryRun ? true : undefined,
    mode,
  });
  console.log(`Run ${result.runId}`);
  console.log(JSON.stringify(result.summary, null, 2));
}

program
  .command("discover")
  .option("--limit <number>", "maximum candidates to inspect", positiveInteger, 50)
  .option("--category <category>", "focus category")
  .action(async (options) => {
    const result = await new OutreachService().discover(options.limit, getCategory(options.category));
    console.table([result]);
  });

program
  .command("research")
  .option("--limit <number>", "maximum prospects to research", positiveInteger, 30)
  .action(async (options) => {
    const service = new OutreachService();
    console.table([await service.research(options.limit)]);
  });

program
  .command("score")
  .option("--limit <number>", "maximum prospects to score", positiveInteger, 200)
  .action((options) => console.table([new OutreachService().score(options.limit)]));

program
  .command("draft")
  .option("--count <number>", "maximum approved prospects to draft", positiveInteger, 17)
  .action(async (options) => {
    const result = await new OutreachService().draftApproved(options.count, { dryRun: true });
    console.table(result.rows);
    console.log(`Local drafts ${result.drafted}; failed ${result.failed}.`);
  });

for (const [command, mode, description] of [
  ["run-local", "local-template", "Run the local deterministic template pipeline."],
  ["run-junglegrid", "junglegrid-template", "Run the template worker on Jungle Grid."],
  ["run-junglegrid-qwen", "junglegrid-qwen", "Run the Qwen/Ollama worker on Jungle Grid."],
] as const) {
  program
    .command(command)
    .description(description)
    .option("--count <number>", "target validated drafts", positiveInteger, 17)
    .option("--category <category>", "focus category")
    .option("--dry-run", "retain dry-run safety setting")
    .action((options) => executeRun(mode, options));
}

program
  .command("test-junglegrid-qwen")
  .description("Estimate the Qwen worker contract without starting a paid job.")
  .option("--count <number>", "small estimate target", positiveInteger, 1)
  .action(async (options) => {
    const estimate = await new JungleGridWorkloadProvider().estimate(
      "junglegrid-qwen",
      Math.min(options.count, 3),
      new OutreachService().repository.getWorkerExclusions(),
    );
    console.log(JSON.stringify(estimate, null, 2));
  });

program.command("list").action(() => {
  const prospects = new OutreachService().repository.listProspects({ limit: 200 });
  console.table(
    prospects.map((prospect) => ({
      id: prospect.id,
      name: prospect.name,
      email: prospect.email,
      project: prospect.project,
      category: prospect.category,
      score: prospect.fitScore ?? "-",
      status: prospect.status,
    })),
  );
});

program.command("status").action(() => {
  const repository = new OutreachService().repository;
  console.log(JSON.stringify(repository.dashboardSummary(), null, 2));
  console.table(
    repository.listRuns(10).map((run) => ({
      id: run.id,
      mode: run.mode,
      jobId: run.junglegridJobId ?? "local",
      phase: run.phase,
      drafted: run.draftedCount,
      failed: run.failedCount,
      created: run.createdAt,
    })),
  );
});

program
  .command("export")
  .option("--format <format>", "json or csv", "json")
  .option("--output <path>", "output file")
  .action((options) => {
    const prospects = new OutreachService().repository.listProspects({ limit: 1000 });
    const format = String(options.format).toLowerCase();
    if (!["json", "csv"].includes(format)) throw new Error("Format must be json or csv.");
    const outputPath = path.resolve(
      options.output ?? `exports/prospects-${new Date().toISOString().slice(0, 10)}.${format}`,
    );
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    const content =
      format === "json"
        ? JSON.stringify(prospects, null, 2)
        : [
            ["id", "name", "email", "project", "category", "fit_score", "status", "source_url"],
            ...prospects.map((prospect) => [
              prospect.id,
              prospect.name,
              prospect.email,
              prospect.project,
              prospect.category,
              prospect.fitScore ?? "",
              prospect.status,
              prospect.emailSourceUrl,
            ]),
          ]
            .map((row) => row.map(csvCell).join(","))
            .join("\n");
    fs.writeFileSync(outputPath, content, "utf8");
    console.log(outputPath);
  });

program
  .command("zeptomail-test")
  .description("Send a ZeptoMail test email to ZEPTOMAIL_TEST_RECIPIENT.")
  .action(async () => {
    const status = getZeptoMailStatus();
    console.table([status]);
    if (!status.sendEnabled) {
      process.exitCode = 1;
      return;
    }
    const result = await new ZeptoMailService().sendTest();
    console.log(JSON.stringify(result, null, 2));
  });

program
  .command("jg-logs")
  .requiredOption("--job-id <id>", "Jungle Grid job ID")
  .action(async (options) => {
    console.log(JSON.stringify(await new JungleGridWorkloadProvider().getLogs(options.jobId), null, 2));
  });

program
  .command("jg-artifacts")
  .requiredOption("--job-id <id>", "Jungle Grid job ID")
  .action(async (options) => {
    console.log(
      JSON.stringify(await new JungleGridWorkloadProvider().listArtifacts(options.jobId), null, 2),
    );
  });

program
  .command("run")
  .option("--mode <mode>", "execution mode", "junglegrid-qwen")
  .option("--count <number>", "target validated drafts", positiveInteger, 17)
  .option("--category <category>", "focus category")
  .action((options) =>
    executeRun(outreachModeSchema.parse(options.mode), {
      count: options.count,
      category: options.category,
    }),
  );

function csvCell(value: unknown): string {
  return `"${String(value ?? "").replaceAll('"', '""')}"`;
}

program.parseAsync().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});

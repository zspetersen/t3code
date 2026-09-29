import { expect, it } from "@effect/vitest";
import { ProviderInstanceId, type TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { makeCopilotTextGeneration } from "./CopilotTextGeneration.ts";

it.effect("rejects implicit helper work without starting a Copilot session", () =>
  Effect.gen(function* () {
    const service = yield* makeCopilotTextGeneration;
    const input = {
      cwd: "/real-workspace",
      message: "Fix a bug",
      branch: "feature",
      stagedSummary: "One file",
      stagedPatch: "Some patch",
      baseBranch: "main",
      headBranch: "feature",
      commitSummary: "Fix",
      diffSummary: "One file",
      diffPatch: "Some patch",
      modelSelection: { instanceId: ProviderInstanceId.make("copilot"), model: "auto" },
    };
    for (const operation of [
      "generateBranchName",
      "generateThreadTitle",
      "generateCommitMessage",
      "generatePrContent",
    ] as const) {
      const request: Effect.Effect<unknown, TextGenerationError> = service[operation](input);
      const result = yield* request.pipe(Effect.result);
      expect(result).toMatchObject({ failure: { _tag: "TextGenerationError", operation } });
    }
  }),
);

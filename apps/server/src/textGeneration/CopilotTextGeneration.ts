import { TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as TextGeneration from "./TextGeneration.ts";

const unavailable = (operation: keyof TextGeneration.TextGeneration["Service"]) =>
  Effect.fail(
    new TextGenerationError({
      operation,
      detail:
        "GitHub Copilot text generation is unavailable in this experiment. Select another provider for the system model.",
    }),
  );

// Copilot has no verified way to disable all configured hooks and MCP servers before
// session startup. Implicit helpers must not execute them against a user's workspace.
export const makeCopilotTextGeneration = Effect.succeed<TextGeneration.TextGeneration["Service"]>({
  generateCommitMessage: () => unavailable("generateCommitMessage"),
  generatePrContent: () => unavailable("generatePrContent"),
  generateBranchName: () => unavailable("generateBranchName"),
  generateThreadTitle: () => unavailable("generateThreadTitle"),
} satisfies TextGeneration.TextGeneration["Service"]);

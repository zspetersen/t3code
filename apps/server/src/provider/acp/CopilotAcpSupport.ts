import type {
  CopilotSettings,
  ProviderApprovalDecision,
  ProviderApprovalOption,
  ProviderInteractionMode,
  ProviderOptionSelection,
  RuntimeMode,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import type * as Acp from "effect-acp/schema";
import { expandHomePath } from "../../pathExpansion.ts";
import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

export function buildCopilotAcpSpawnInput(
  settings: CopilotSettings,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
  _runtimeMode?: RuntimeMode,
): AcpSessionRuntime.AcpSpawnInput {
  return {
    command: expandHomePath(settings.binaryPath || "copilot"),
    args: ["--acp", "--stdio", "--no-auto-update"],
    cwd,
    env: {
      ...(environment ?? process.env),
      ...(settings.homePath ? { COPILOT_HOME: expandHomePath(settings.homePath) } : {}),
    },
    extendEnv: false,
  };
}

export interface CopilotAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  "spawn" | "authMethodId"
> {
  readonly copilotSettings: CopilotSettings;
  readonly environment?: NodeJS.ProcessEnv;
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly runtimeMode?: RuntimeMode;
}

/** Copilot authenticate checks existing credentials; sign-in is an explicit CLI action. */
export const makeCopilotAcpRuntime = Effect.fn("makeCopilotAcpRuntime")(function* (
  input: CopilotAcpRuntimeInput,
) {
  const context = yield* Layer.build(
    AcpSessionRuntime.layer({
      ...input,
      spawn: buildCopilotAcpSpawnInput(input.copilotSettings, input.cwd, input.environment),
      authMethodId: "copilot-login",
      resumeMethod: "load",
      cancelBehavior: "wait-for-prompt",
    }).pipe(
      Layer.provide(
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
      ),
    ),
  );
  return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(Effect.provide(context));
});

const permissionKinds = {
  accept: "allow_once",
  acceptForSession: "allow_always",
  decline: "reject_once",
} as const;

/** Copilot's ACP allow_always remembers the rule only for the native session. */
export function copilotApprovalOptions(
  request: Acp.RequestPermissionRequest,
): ReadonlyArray<ProviderApprovalOption> {
  return request.options.flatMap((option): ReadonlyArray<ProviderApprovalOption> => {
    switch (option.kind) {
      case "allow_once":
        return [{ decision: "accept" as const, label: option.name }];
      case "allow_always":
        return [{ decision: "acceptForSession" as const, label: "Allow for session" }];
      case "reject_once":
        return [{ decision: "decline" as const, label: option.name }];
      default:
        return [];
    }
  });
}

export function selectCopilotPermissionOption(
  request: Acp.RequestPermissionRequest,
  decision: ProviderApprovalDecision,
): string | undefined {
  if (decision === "cancel" || decision === "acceptAlways") return undefined;
  return request.options.find((option) => option.kind === permissionKinds[decision])?.optionId;
}

function selectOptions(option: Acp.SessionConfigOption | undefined) {
  return option?.type === "select"
    ? option.options.flatMap((entry) => ("value" in entry ? [entry] : entry.options))
    : [];
}

/** Modes can reset permission state, so apply allow_all after every mode change. */
export const applyCopilotSessionConfiguration = Effect.fn("applyCopilotSessionConfiguration")(
  function* (input: {
    readonly runtime: Pick<
      AcpSessionRuntime.AcpSessionRuntime["Service"],
      "getConfigOptions" | "setConfigOption" | "setSessionModel"
    >;
    readonly runtimeMode: RuntimeMode;
    readonly interactionMode?: ProviderInteractionMode | undefined;
    readonly model?: string | undefined;
    readonly selections?: ReadonlyArray<ProviderOptionSelection> | undefined;
  }) {
    const options = yield* input.runtime.getConfigOptions;
    const modelConfig = options.find(
      (option) => option.id === "model" || option.category === "model",
    );
    let model = input.model;
    // The fallback catalog alias means the CLI's default when auto isn't advertised.
    if (model === "auto" && !selectOptions(modelConfig).some((option) => option.value === "auto")) {
      model = modelConfig?.type === "select" ? modelConfig.currentValue : undefined;
    }
    if (model) {
      if (modelConfig) yield* input.runtime.setConfigOption(modelConfig.id, model);
      else yield* input.runtime.setSessionModel(model);
    }
    for (const selection of input.selections ?? []) {
      if (selection.id !== "reasoning_effort" && selection.id !== "agent") continue;
      yield* input.runtime.setConfigOption(selection.id, selection.value);
    }
    const modeConfig = options.find((option) => option.id === "mode");
    const requestedMode = input.interactionMode === "plan" ? "plan" : "agent";
    const mode = selectOptions(modeConfig).find(
      (option) => option.value === requestedMode || option.value.endsWith(`#${requestedMode}`),
    );
    if (mode) yield* input.runtime.setConfigOption("mode", mode.value);
    const permissions = options.find((option) => option.id === "allow_all");
    if (permissions) {
      yield* input.runtime.setConfigOption(
        "allow_all",
        permissions.type === "boolean"
          ? input.runtimeMode === "full-access"
          : input.runtimeMode === "full-access"
            ? "on"
            : "off",
      );
    }
    return model;
  },
);

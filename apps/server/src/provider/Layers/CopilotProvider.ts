import {
  ProviderDriverKind,
  type CopilotSettings,
  type ServerProvider,
  type ServerProviderModel,
  type ServerProviderSlashCommand,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import type * as EffectAcpSchema from "effect-acp/schema";

import type { AcpSessionRuntimeStartResult } from "../acp/AcpSessionRuntime.ts";
import { buildCopilotAcpSpawnInput, makeCopilotAcpRuntime } from "../acp/CopilotAcpSupport.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const EMPTY_CAPABILITIES = createModelCapabilities({ optionDescriptors: [] });
const SIGN_IN_MESSAGE =
  "Run `copilot login` on the server host, then refresh GitHub Copilot models.";
const MAX_WORKSPACE_SNAPSHOTS = 16;

type SessionCatalog = Pick<
  AcpSessionRuntimeStartResult["sessionSetupResult"],
  "configOptions" | "models"
>;

interface CopilotProviderState {
  readonly draft: ServerProviderDraft;
  readonly authRevision: number;
}

/** Use native account model IDs; `auto` delegates model choice to the CLI. */
export function buildCopilotModelsFromSession(
  setup: SessionCatalog,
): ReadonlyArray<ServerProviderModel> {
  const modelConfig = setup.configOptions?.find(
    (option) => option.id === "model" || option.category === "model",
  );
  const current =
    modelConfig?.type === "select" ? modelConfig.currentValue : setup.models?.currentModelId;
  const entries =
    modelConfig?.type === "select"
      ? modelConfig.options.flatMap((option) => ("value" in option ? [option] : option.options))
      : (setup.models?.availableModels.map((model) => ({
          value: model.modelId,
          name: model.name,
        })) ?? []);
  const seen = new Set<string>();
  return entries.flatMap((entry): ServerProviderModel[] => {
    const slug = entry.value.trim();
    if (!slug || seen.has(slug)) return [];
    seen.add(slug);
    return [
      {
        slug,
        name: entry.name.trim() || slug,
        isCustom: false,
        capabilities: EMPTY_CAPABILITIES,
        ...(slug === current ? { isDefault: true, aliases: ["auto"] } : {}),
      },
    ];
  });
}

function retainCatalogDefault(
  models: ReadonlyArray<ServerProviderModel>,
  previous: ReadonlyArray<ServerProviderModel>,
): ReadonlyArray<ServerProviderModel> {
  const stable = previous.find((model) => model.isDefault && !model.isCustom);
  if (!stable || !models.some((model) => model.slug === stable.slug)) return models;
  return models.map((model) => {
    const { isDefault: _isDefault, aliases: _aliases, ...rest } = model;
    return model.slug === stable.slug ? { ...rest, isDefault: true, aliases: ["auto"] } : rest;
  });
}

function modelsFromSettings(
  settings: CopilotSettings,
  models?: ReadonlyArray<ServerProviderModel>,
) {
  return providerModelsFromSettings(
    models && models.length > 0
      ? models
      : [
          {
            slug: "auto",
            name: "Auto",
            isCustom: false,
            isDefault: true,
            capabilities: EMPTY_CAPABILITIES,
          },
        ],
    settings.customModels,
    EMPTY_CAPABILITIES,
  );
}

export const buildInitialCopilotProviderSnapshot = Effect.fn("buildInitialCopilotProviderSnapshot")(
  function* (settings: CopilotSettings) {
    return {
      ...buildServerProvider({
        presentation: {
          displayName: "GitHub Copilot",
          badgeLabel: "Experimental",
          supportsConversationRollback: false,
          showInteractionModeToggle: false,
        },
        enabled: settings.enabled,
        checkedAt: DateTime.formatIso(yield* DateTime.now),
        models: modelsFromSettings(settings),
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: settings.enabled
            ? "Checking GitHub Copilot CLI availability."
            : "GitHub Copilot is disabled in T3 Code settings.",
        },
      }),
      supportsTextGeneration: false,
      setup: { canAuthenticate: false, canInstall: false },
      workspaceSnapshots: [],
    } satisfies ServerProviderDraft;
  },
);

/** Background checks never authenticate or create sessions, which can start hooks or MCP servers. */
export const checkCopilotProviderStatus = Effect.fn("checkCopilotProviderStatus")(function* (
  settings: CopilotSettings,
  environment?: NodeJS.ProcessEnv,
) {
  const initial = yield* buildInitialCopilotProviderSnapshot(settings);
  if (!settings.enabled) return initial;

  const result = yield* Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-copilot-health-" });
    const spawn = buildCopilotAcpSpawnInput(settings, cwd, environment);
    const command = yield* resolveSpawnCommand(
      spawn.command,
      ["--version"],
      spawn.env ? { env: spawn.env } : {},
    );
    const version = yield* spawnAndCollect(
      spawn.command,
      ChildProcess.make(command.command, command.args, {
        cwd,
        env: spawn.env,
        extendEnv: false,
        shell: command.shell,
      }),
    );
    if (version.code !== 0) return { version: null, initialized: false };
    const runtime = yield* makeCopilotAcpRuntime({
      copilotSettings: settings,
      ...(environment ? { environment } : {}),
      childProcessSpawner: spawner,
      cwd,
      clientInfo: { name: "t3-code-health", version: "0.0.0" },
    });
    const initialized = yield* runtime.initialize();
    return {
      version: initialized.agentInfo?.version || parseGenericCliVersion(version.stdout),
      initialized: true,
    };
  }).pipe(Effect.scoped, Effect.timeoutOption("15 seconds"), Effect.result);

  if (Result.isFailure(result)) {
    const failure = result.failure;
    const missing =
      isCommandMissingCause(failure) ||
      (failure._tag === "AcpSpawnError" && isCommandMissingCause(failure.cause));
    return {
      ...initial,
      installed: !missing,
      status: "error" as const,
      message: missing
        ? "GitHub Copilot CLI is not installed or its executable was not found on the server host. Install `@github/copilot`, then run `copilot login`."
        : "GitHub Copilot CLI could not complete its local health check. Check the executable path and update the CLI on the server host.",
    };
  }
  if (Option.isNone(result.success) || !result.success.value.initialized) {
    return {
      ...initial,
      installed: true,
      status: "error" as const,
      message:
        "GitHub Copilot CLI did not complete its local health check. Check the CLI on the server host.",
    };
  }
  return {
    ...initial,
    installed: true,
    version: result.success.value.version,
    message: `GitHub Copilot is installed. Account access has not been checked. ${SIGN_IN_MESSAGE}`,
  };
});

/** Health owns installation status; explicit sessions supply authenticated catalogs. */
export const makeCopilotProvider = Effect.fn("makeCopilotProvider")(function* (
  settings: CopilotSettings,
  options: {
    readonly stampIdentity: (snapshot: ServerProviderDraft) => ServerProvider;
    readonly probe: Effect.Effect<ServerProviderDraft>;
  },
) {
  const initial = yield* buildInitialCopilotProviderSnapshot(settings);
  const metadata = yield* SubscriptionRef.make<CopilotProviderState>({
    draft: initial,
    authRevision: 0,
  });
  const getSnapshot = SubscriptionRef.get(metadata).pipe(
    Effect.map((state) => options.stampIdentity(state.draft)),
  );
  const checkProvider = Effect.gen(function* () {
    if (!settings.enabled) return yield* getSnapshot;
    const before = yield* SubscriptionRef.get(metadata);
    const probe = yield* options.probe;
    const updated = yield* SubscriptionRef.updateAndGet(metadata, (state) => {
      if (state.authRevision !== before.authRevision) return state;
      if (!probe.installed) return { draft: probe, authRevision: state.authRevision + 1 };
      const { message: _message, ...draft } = state.draft;
      const authenticated = draft.auth.status === "authenticated";
      return {
        ...state,
        draft: {
          ...draft,
          installed: probe.installed,
          version: probe.version ?? draft.version,
          checkedAt: probe.checkedAt,
          status: probe.status === "error" ? "error" : authenticated ? "ready" : "warning",
          ...(probe.status === "error" || !authenticated
            ? { message: draft.auth.status === "unauthenticated" ? SIGN_IN_MESSAGE : probe.message }
            : {}),
        },
      } satisfies CopilotProviderState;
    });
    return options.stampIdentity(updated.draft);
  });
  const maintenance = makeManualOnlyProviderMaintenanceCapabilities({
    provider: ProviderDriverKind.make("githubCopilot"),
    packageName: "@github/copilot",
  });
  const managed = yield* makeManagedServerProvider({
    resolveMaintenance: () => Effect.succeed(maintenance),
    getSettings: Effect.succeed(settings),
    streamSettings: Stream.empty,
    haveSettingsChanged: () => false,
    initialSnapshot: () => getSnapshot,
    checkProvider,
    enrichSnapshot: ({ publishSnapshot }) =>
      SubscriptionRef.changes(metadata).pipe(
        Stream.map((state) => options.stampIdentity(state.draft)),
        Stream.runForEach(publishSnapshot),
      ),
  });

  const onSessionStarted = Effect.fn("CopilotProvider.onSessionStarted")(function* (
    started: AcpSessionRuntimeStartResult,
    _cwd?: string,
  ) {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const nativeModels = buildCopilotModelsFromSession(started.sessionSetupResult);
    yield* SubscriptionRef.update(metadata, (state) => {
      const { message: _message, ...draft } = state.draft;
      return {
        authRevision: state.authRevision + 1,
        draft: {
          ...draft,
          installed: true,
          status: settings.enabled ? "ready" : "disabled",
          auth: { status: "authenticated", type: "copilot-login", label: "GitHub account" },
          version: started.initializeResult.agentInfo?.version || draft.version,
          checkedAt,
          models:
            nativeModels.length > 0
              ? modelsFromSettings(
                  settings,
                  state.draft.auth.status === "authenticated"
                    ? retainCatalogDefault(nativeModels, state.draft.models)
                    : nativeModels,
                )
              : state.draft.auth.status === "authenticated"
                ? state.draft.models
                : modelsFromSettings(settings),
        },
      } satisfies CopilotProviderState;
    });
  });
  const onConfigOptionsUpdated = (
    configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption>,
  ) => {
    const models = buildCopilotModelsFromSession({ configOptions });
    if (models.length === 0) return Effect.void;
    return SubscriptionRef.update(metadata, (state) =>
      state.draft.auth.status === "unauthenticated"
        ? state
        : {
            ...state,
            draft: {
              ...state.draft,
              models: modelsFromSettings(
                settings,
                retainCatalogDefault(models, state.draft.models),
              ),
            },
          },
    );
  };
  const onAvailableCommands = Effect.fn("CopilotProvider.onAvailableCommands")(function* (
    commands: ReadonlyArray<EffectAcpSchema.AvailableCommand>,
    cwd: string,
  ) {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const seen = new Set<string>();
    const slashCommands = commands.flatMap((command): ServerProviderSlashCommand[] => {
      const name = command.name.trim();
      if (!name || seen.has(name)) return [];
      seen.add(name);
      const description = command.description.trim();
      const hint = command.input?.hint.trim();
      return [
        { name, ...(description ? { description } : {}), ...(hint ? { input: { hint } } : {}) },
      ];
    });
    yield* SubscriptionRef.update(metadata, (state) =>
      state.draft.auth.status === "unauthenticated"
        ? state
        : {
            ...state,
            draft: {
              ...state.draft,
              workspaceSnapshots: [
                ...(state.draft.workspaceSnapshots ?? []).filter((entry) => entry.cwd !== cwd),
                { cwd, checkedAt, slashCommands, skills: [] },
              ].slice(-MAX_WORKSPACE_SNAPSHOTS),
            },
          },
    );
  });
  const onAuthRequired = SubscriptionRef.update(
    metadata,
    (state) =>
      ({
        authRevision: state.authRevision + 1,
        draft: {
          ...state.draft,
          auth: { status: "unauthenticated" },
          status: settings.enabled ? "warning" : "disabled",
          message: SIGN_IN_MESSAGE,
          models: modelsFromSettings(settings),
          slashCommands: [],
          workspaceSnapshots: [],
        },
      }) satisfies CopilotProviderState,
  );
  const snapshotForCwd = (cwd: string) =>
    getSnapshot.pipe(
      Effect.map((snapshot) => ({
        ...snapshot,
        slashCommands:
          snapshot.workspaceSnapshots?.find((entry) => entry.cwd === cwd)?.slashCommands ?? [],
      })),
    );

  return {
    snapshot: { ...managed, getSnapshot },
    snapshotForCwd,
    onSessionStarted,
    onConfigOptionsUpdated,
    onAvailableCommands,
    onAuthRequired,
  };
});

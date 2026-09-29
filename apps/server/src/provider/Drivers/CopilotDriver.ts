import { CopilotSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { makeCopilotTextGeneration } from "../../textGeneration/CopilotTextGeneration.ts";
import { makeCopilotAcpRuntime } from "../acp/CopilotAcpSupport.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeCopilotAdapter } from "../Layers/CopilotAdapter.ts";
import { checkCopilotProviderStatus, makeCopilotProvider } from "../Layers/CopilotProvider.ts";
import { ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";

const DRIVER = ProviderDriverKind.make("githubCopilot");
const decodeSettings = Schema.decodeSync(CopilotSettings);

export type CopilotDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
  | ProviderEventLoggers
  | ServerConfig
  | ServerSettingsService;

export const CopilotDriver: ProviderDriver<CopilotSettings, CopilotDriverEnv> = {
  driverKind: DRIVER,
  metadata: {
    displayName: "GitHub Copilot",
    // A separate COPILOT_HOME has not established isolation from the host keychain.
    supportsMultipleInstances: false,
  },
  configSchema: CopilotSettings,
  defaultConfig: () => decodeSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fs = yield* FileSystem.FileSystem;
      const eventLoggers = yield* ProviderEventLoggers;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const settings = { ...config, enabled } satisfies CopilotSettings;
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const provider = yield* makeCopilotProvider(settings, {
        stampIdentity,
        probe: checkCopilotProviderStatus(settings, processEnv).pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provideService(FileSystem.FileSystem, fs),
        ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER,
              instanceId,
              detail: "Failed to initialize GitHub Copilot provider status.",
              cause,
            }),
        ),
      );
      const adapter = yield* makeCopilotAdapter(settings, {
        instanceId,
        environment: processEnv,
        ...(eventLoggers.native ? { nativeEventLogger: eventLoggers.native } : {}),
        onSessionStarted: provider.onSessionStarted,
        onAvailableCommands: provider.onAvailableCommands,
        onConfigOptionsUpdated: provider.onConfigOptionsUpdated,
        onAuthRequired: provider.onAuthRequired,
      });
      const textGeneration = yield* makeCopilotTextGeneration;
      const refreshModels = Effect.fn("CopilotDriver.refreshModels")(
        function* () {
          if (!settings.enabled) return;
          const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-copilot-models-" });
          const runtime = yield* makeCopilotAcpRuntime({
            copilotSettings: settings,
            environment: processEnv,
            childProcessSpawner: spawner,
            cwd,
            clientInfo: { name: "t3-code-model-discovery", version: "0.0.0" },
            mcpServers: [],
          }).pipe(Effect.provideService(Crypto.Crypto, crypto));
          const started = yield* runtime.start();
          yield* provider.onSessionStarted(started);
        },
        Effect.scoped,
        Effect.timeoutOrElse({
          duration: "30 seconds",
          orElse: () =>
            Effect.fail(
              new ProviderDriverError({
                driver: DRIVER,
                instanceId,
                detail:
                  "GitHub Copilot model refresh timed out. Check the CLI and login on the server host.",
              }),
            ),
        }),
        Effect.tapError((cause) =>
          cause._tag === "AcpRequestError" && cause.code === -32000
            ? provider.onAuthRequired
            : Effect.void,
        ),
        Effect.mapError((cause) =>
          cause._tag === "ProviderDriverError"
            ? cause
            : new ProviderDriverError({
                driver: DRIVER,
                instanceId,
                detail:
                  "Could not refresh GitHub Copilot models. Run `copilot login` on the server host and try again.",
                cause,
              }),
        ),
      );
      return {
        instanceId,
        driverKind: DRIVER,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot: provider.snapshot,
        snapshotForCwd: provider.snapshotForCwd,
        refreshModels,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};

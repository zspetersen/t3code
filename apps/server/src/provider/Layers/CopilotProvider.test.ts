// @effect-diagnostics preferSchemaOverJson:off - mock CLI source and captured protocol fixtures.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { ProviderDriverKind, ProviderInstanceId, type CopilotSettings } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import type { AcpSessionRuntimeStartResult } from "../acp/AcpSessionRuntime.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import {
  buildCopilotModelsFromSession,
  buildInitialCopilotProviderSnapshot,
  checkCopilotProviderStatus,
  makeCopilotProvider,
} from "./CopilotProvider.ts";

const settings = {
  enabled: true,
  binaryPath: "copilot",
  homePath: "",
  customModels: [],
} satisfies CopilotSettings;
const instanceId = ProviderInstanceId.make("copilot-test");
const driver = ProviderDriverKind.make("githubCopilot");
const started = {
  sessionId: "copilot-session",
  initializeResult: {
    protocolVersion: 1,
    agentCapabilities: { loadSession: true, promptCapabilities: { image: true } },
    agentInfo: { name: "GitHub Copilot", version: "1.0.83" },
    authMethods: [{ id: "copilot-login", name: "Log in to GitHub" }],
  },
  sessionSetupResult: {
    sessionId: "copilot-session",
    models: {
      currentModelId: "native-model-b",
      availableModels: [
        { modelId: "native-model-a", name: "First model" },
        { modelId: "native-model-b", name: "Second model" },
      ],
    },
  },
  modelConfigId: undefined,
} satisfies AcpSessionRuntimeStartResult;

const testLayer = Layer.merge(
  Layer.mock(BackgroundPolicy.BackgroundPolicy)({
    shouldRunScopeWork: () => Effect.succeed(false),
  }),
  ServerSettingsService.layerTest(),
);

describe("Copilot model discovery", () => {
  it("uses the native account catalog and aliases its current model as auto", () => {
    expect(buildCopilotModelsFromSession(started.sessionSetupResult)).toMatchObject([
      { slug: "native-model-a", name: "First model", isCustom: false },
      { slug: "native-model-b", name: "Second model", isDefault: true, aliases: ["auto"] },
    ]);
  });

  it("uses grouped config options ahead of legacy models and drops empty or repeated IDs", () => {
    const models = buildCopilotModelsFromSession({
      ...started.sessionSetupResult,
      configOptions: [
        {
          id: "model",
          category: "model",
          name: "Model",
          type: "select",
          currentValue: "new-model",
          options: [
            {
              group: "new",
              name: "Current models",
              options: [
                { value: "new-model", name: "New model" },
                { value: "new-model", name: "Repeated model" },
                { value: "", name: "Invalid model" },
              ],
            },
          ],
        },
      ],
    });
    expect(models.map((model) => model.slug)).toEqual(["new-model"]);
    expect(models[0]).toMatchObject({ isDefault: true, aliases: ["auto"] });
  });
});

describe("Copilot health probe", () => {
  it.effect("disabled providers never start the CLI", () =>
    Effect.gen(function* () {
      const result = yield* checkCopilotProviderStatus({ ...settings, enabled: false });
      expect(result.status).toBe("disabled");
      expect(result.auth.status).toBe("unknown");
      expect(result.supportsTextGeneration).toBe(false);
    }).pipe(
      Effect.provide(NodeServices.layer),
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Disabled Copilot must not spawn")),
      ),
    ),
  );

  it.effect("only checks version and initialize, carrying the configured host environment", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-copilot-status-" });
      const requestLog = path.join(directory, "requests.ndjson");
      const binaryPath = writeFakeCli({
        directory,
        name: "copilot",
        source: [
          'import { appendFileSync } from "node:fs";',
          'import { createInterface } from "node:readline";',
          'const log = (value) => appendFileSync(process.env.REQUEST_LOG, JSON.stringify(value) + "\\n");',
          'if (process.argv.includes("--version")) {',
          '  log({ method: "version", home: process.env.COPILOT_HOME, marker: process.env.INSTANCE_MARKER });',
          '  process.stdout.write("GitHub Copilot CLI 1.0.83\\n");',
          "  process.exit(0);",
          "}",
          'createInterface({ input: process.stdin }).on("line", (line) => {',
          "  const request = JSON.parse(line);",
          "  log({ method: request.method, home: process.env.COPILOT_HOME, marker: process.env.INSTANCE_MARKER });",
          `  const result = ${JSON.stringify(started.initializeResult)};`,
          '  process.stdout.write(JSON.stringify(request.method === "initialize"',
          '    ? { jsonrpc: "2.0", id: request.id, result }',
          '    : { jsonrpc: "2.0", id: request.id, error: { code: -32000, message: "Unexpected session or authentication" } }) + "\\n");',
          "});",
        ].join("\n"),
      });
      const homePath = path.join(directory, "copilot-home");
      const result = yield* checkCopilotProviderStatus(
        { ...settings, binaryPath, homePath },
        { ...process.env, REQUEST_LOG: requestLog, INSTANCE_MARKER: "test-instance" },
      );
      expect(result).toMatchObject({
        installed: true,
        version: "1.0.83",
        auth: { status: "unknown" },
        supportsConversationRollback: false,
        supportsTextGeneration: false,
      });
      const requests = (yield* fs.readFileString(requestLog))
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line));
      expect(requests).toEqual([
        { method: "version", home: homePath, marker: "test-instance" },
        { method: "initialize", home: homePath, marker: "test-instance" },
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("reports a missing executable with host installation guidance", () =>
    Effect.gen(function* () {
      const result = yield* checkCopilotProviderStatus({
        ...settings,
        binaryPath: "/t3-nonexistent/copilot",
      });
      expect(result.installed).toBe(false);
      expect(result.status).toBe("error");
      expect(result.message).toMatch(/install|not found/i);
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

it.layer(testLayer)("Copilot live catalog", (it) => {
  it.effect(
    "keeps session metadata over health refreshes and commands scoped to their workspace",
    () =>
      Effect.gen(function* () {
        const initial = yield* buildInitialCopilotProviderSnapshot(settings);
        const probe = yield* Ref.make({ ...initial, installed: true });
        const provider = yield* makeCopilotProvider(settings, {
          stampIdentity: (snapshot) => ({ ...snapshot, instanceId, driver }),
          probe: Ref.get(probe),
        });
        yield* provider.onSessionStarted(started, "/project-a");
        yield* provider.onAvailableCommands(
          [{ name: "project-command", description: "Project command" }],
          "/project-a",
        );
        yield* provider.snapshot.refresh;
        const snapshot = yield* provider.snapshot.getSnapshot;
        expect(snapshot.auth.status).toBe("authenticated");
        expect(snapshot.models.map((model) => model.slug)).toEqual([
          "native-model-a",
          "native-model-b",
        ]);
        expect((yield* provider.snapshotForCwd("/project-a")).slashCommands).toEqual([
          { name: "project-command", description: "Project command" },
        ]);
        expect((yield* provider.snapshotForCwd("/project-b")).slashCommands).toEqual([]);
        yield* provider.onConfigOptionsUpdated([]);
        expect((yield* provider.snapshot.getSnapshot).models).toEqual(snapshot.models);
      }).pipe(Effect.scoped),
  );

  it.effect("publishes catalog changes without waiting for another health check", () =>
    Effect.gen(function* () {
      const initial = yield* buildInitialCopilotProviderSnapshot(settings);
      const provider = yield* makeCopilotProvider(settings, {
        stampIdentity: (snapshot) => ({ ...snapshot, instanceId, driver }),
        probe: Effect.succeed({ ...initial, installed: true }),
      });
      const listening = yield* Deferred.make<void>();
      const update = yield* provider.snapshot.streamChanges.pipe(
        Stream.onStart(Deferred.succeed(listening, undefined)),
        Stream.filter((snapshot) => snapshot.auth.status === "authenticated"),
        Stream.take(1),
        Stream.runHead,
        Effect.forkScoped,
      );
      yield* Deferred.await(listening);
      yield* provider.onSessionStarted(started, "/project-a");
      expect(yield* Fiber.join(update)).toMatchObject({
        value: { auth: { status: "authenticated" } },
      });
    }).pipe(Effect.scoped),
  );

  it.effect("retains discovered models when a resumed session omits its catalog", () =>
    Effect.gen(function* () {
      const initial = yield* buildInitialCopilotProviderSnapshot(settings);
      const provider = yield* makeCopilotProvider(settings, {
        stampIdentity: (snapshot) => ({ ...snapshot, instanceId, driver }),
        probe: Effect.succeed({ ...initial, installed: true }),
      });
      yield* provider.onSessionStarted(started);
      yield* provider.onSessionStarted({
        ...started,
        sessionSetupResult: { sessionId: started.sessionId },
      });
      expect((yield* provider.snapshot.getSnapshot).models.map((model) => model.slug)).toEqual([
        "native-model-a",
        "native-model-b",
      ]);
    }).pipe(Effect.scoped),
  );

  it.effect("does not change the instance default when one session changes models", () =>
    Effect.gen(function* () {
      const initial = yield* buildInitialCopilotProviderSnapshot(settings);
      const provider = yield* makeCopilotProvider(settings, {
        stampIdentity: (snapshot) => ({ ...snapshot, instanceId, driver }),
        probe: Effect.succeed({ ...initial, installed: true }),
      });
      yield* provider.onSessionStarted(started);
      yield* provider.onConfigOptionsUpdated([
        {
          id: "model",
          name: "Model",
          type: "select",
          currentValue: "native-model-a",
          options: [
            { value: "native-model-a", name: "First model" },
            { value: "native-model-b", name: "Second model" },
          ],
        },
      ]);
      const models = (yield* provider.snapshot.getSnapshot).models;
      expect(models.find((model) => model.isDefault)?.slug).toBe("native-model-b");
    }).pipe(Effect.scoped),
  );

  it.effect(
    "does not change the instance default when another session starts with a different model",
    () =>
      Effect.gen(function* () {
        const initial = yield* buildInitialCopilotProviderSnapshot(settings);
        const provider = yield* makeCopilotProvider(settings, {
          stampIdentity: (snapshot) => ({ ...snapshot, instanceId, driver }),
          probe: Effect.succeed({ ...initial, installed: true }),
        });
        yield* provider.onSessionStarted(started);
        yield* provider.onSessionStarted({
          ...started,
          sessionSetupResult: {
            ...started.sessionSetupResult,
            models: { ...started.sessionSetupResult.models!, currentModelId: "native-model-a" },
          },
        });
        expect(
          (yield* provider.snapshot.getSnapshot).models.find((model) => model.isDefault)?.slug,
        ).toBe("native-model-b");
      }).pipe(Effect.scoped),
  );

  it.effect(
    "clears account metadata after failed authentication and ignores stale config updates",
    () =>
      Effect.gen(function* () {
        const initial = yield* buildInitialCopilotProviderSnapshot(settings);
        const provider = yield* makeCopilotProvider(settings, {
          stampIdentity: (snapshot) => ({ ...snapshot, instanceId, driver }),
          probe: Effect.succeed({ ...initial, installed: true }),
        });
        yield* provider.onSessionStarted(started);
        yield* provider.onAvailableCommands(
          [{ name: "workspace-command", description: "Command" }],
          "/project",
        );
        yield* provider.onAuthRequired;
        yield* provider.onAvailableCommands(
          [{ name: "stale-command", description: "Stale command" }],
          "/project",
        );
        yield* provider.onConfigOptionsUpdated([
          {
            id: "model",
            name: "Model",
            type: "select",
            currentValue: "stale-model",
            options: [{ value: "stale-model", name: "Stale model" }],
          },
        ]);
        yield* provider.snapshot.refresh;
        const snapshot = yield* provider.snapshot.getSnapshot;
        expect(snapshot.auth.status).toBe("unauthenticated");
        expect(snapshot.models.map((model) => model.slug)).toEqual(["auto"]);
        expect(snapshot.workspaceSnapshots).toEqual([]);
        expect(snapshot.message).toContain("copilot login");
      }).pipe(Effect.scoped),
  );

  it.effect("keeps model catalogs local to each provider instance", () =>
    Effect.gen(function* () {
      const initial = yield* buildInitialCopilotProviderSnapshot(settings);
      const options = {
        stampIdentity: (snapshot: ServerProviderDraft) => ({ ...snapshot, instanceId, driver }),
        probe: Effect.succeed({ ...initial, installed: true }),
      };
      const first = yield* makeCopilotProvider(settings, options);
      const second = yield* makeCopilotProvider(settings, options);
      yield* first.onSessionStarted(started);
      expect((yield* second.snapshot.getSnapshot).auth.status).toBe("unknown");
      expect((yield* second.snapshot.getSnapshot).models.map((model) => model.slug)).toEqual([
        "auto",
      ]);
    }).pipe(Effect.scoped),
  );
});

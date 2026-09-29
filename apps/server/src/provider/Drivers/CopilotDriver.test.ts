// @effect-diagnostics preferSchemaOverJson:off - captured CLI protocol fixtures.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import { NoOpProviderEventLoggers, ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { CopilotDriver } from "./CopilotDriver.ts";

const testLayer = ServerConfig.layerTest(process.cwd(), { prefix: "t3-copilot-driver-" }).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(ServerSettingsService.layerTest()),
  Layer.provideMerge(
    Layer.mock(BackgroundPolicy.BackgroundPolicy)({
      shouldRunScopeWork: () => Effect.succeed(false),
    }),
  ),
  Layer.provideMerge(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
);

const makeFixture = Effect.fn("makeCopilotDriverFixture")(function* (
  authenticationRequired = false,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-copilot-driver-cli-" });
  const requestLog = path.join(directory, "requests.ndjson");
  const binaryPath = writeFakeCli({
    directory,
    name: "copilot",
    env: { REQUEST_LOG: requestLog },
    source: [
      'import { appendFileSync } from "node:fs";',
      'import { createInterface } from "node:readline";',
      'if (process.argv.includes("--version")) { process.stdout.write("1.0.83\\n"); process.exit(0); }',
      'createInterface({ input: process.stdin }).on("line", (line) => {',
      "  const request = JSON.parse(line);",
      '  appendFileSync(process.env.REQUEST_LOG, JSON.stringify({ method: request.method, cwd: process.cwd() }) + "\\n");',
      "  let result = {};",
      '  if (request.method === "initialize") result = { protocolVersion: 1, agentCapabilities: { loadSession: true }, agentInfo: { name: "copilot", version: "1.0.83" }, authMethods: [{ id: "copilot-login", name: "GitHub" }] };',
      '  if (request.method === "session/new") result = { sessionId: "discovery-session", models: { currentModelId: "account-model", availableModels: [{ modelId: "account-model", name: "Account model" }] } };',
      `  const error = request.method === "authenticate" && ${authenticationRequired} ? { code: -32000, message: "Authentication required" } : undefined;`,
      '  process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: request.id, ...(error ? { error } : { result }) }) + "\\n");',
      "});",
    ].join("\n"),
  });
  const instance = yield* CopilotDriver.create({
    instanceId: ProviderInstanceId.make("copilot-discovery-test"),
    displayName: undefined,
    environment: [],
    enabled: true,
    config: { ...CopilotDriver.defaultConfig(), binaryPath },
  });
  return { fs, requestLog, instance };
});

it.layer(testLayer)("CopilotDriver", (it) => {
  it.effect("keeps the experiment idle until enabled, including explicit model refresh", () =>
    Effect.gen(function* () {
      const instance = yield* CopilotDriver.create({
        instanceId: ProviderInstanceId.make("copilot-test"),
        displayName: undefined,
        environment: [],
        enabled: false,
        config: CopilotDriver.defaultConfig(),
      });
      expect((yield* instance.snapshot.refresh).status).toBe("disabled");
      yield* instance.refreshModels?.() ?? Effect.void;
      expect((yield* instance.snapshot.getSnapshot).models.map((model) => model.slug)).toEqual([
        "auto",
      ]);
      expect((yield* instance.snapshot.resolveMaintenance()).update).toBeNull();
      expect(CopilotDriver.defaultConfig().enabled).toBe(false);
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Disabled Copilot must not spawn a process")),
      ),
      Effect.scoped,
    ),
  );

  it.effect(
    "explicit refresh discovers account models without prompting and removes its temporary workspace",
    () =>
      Effect.gen(function* () {
        const { fs, requestLog, instance } = yield* makeFixture();
        yield* instance.refreshModels?.() ?? Effect.void;
        yield* instance.snapshot.refresh;
        const snapshot = yield* instance.snapshot.getSnapshot;
        expect(snapshot.auth.status).toBe("authenticated");
        expect(snapshot.models.map((model) => model.slug)).toEqual(["account-model"]);
        const requests: Array<{ method: string; cwd: string }> = (yield* fs.readFileString(
          requestLog,
        ))
          .trim()
          .split("\n")
          .map((line) => JSON.parse(line));
        expect(requests.filter((request) => request.method === "authenticate")).toHaveLength(1);
        const sessions = requests.filter((request) => request.method === "session/new");
        expect(sessions).toHaveLength(1);
        expect(requests.some((request) => request.method === "session/prompt")).toBe(false);
        expect(yield* fs.exists(sessions[0]!.cwd)).toBe(false);
      }).pipe(Effect.scoped),
  );

  it.effect(
    "failed explicit authentication publishes sign-in guidance without opening a session",
    () =>
      Effect.gen(function* () {
        const { fs, requestLog, instance } = yield* makeFixture(true);
        const result = yield* (instance.refreshModels?.() ?? Effect.void).pipe(Effect.result);
        expect(result).toMatchObject({ failure: { _tag: "ProviderDriverError" } });
        yield* instance.snapshot.refresh;
        const snapshot = yield* instance.snapshot.getSnapshot;
        expect(snapshot.auth.status).toBe("unauthenticated");
        expect(snapshot.message).toContain("copilot login");
        const requests: Array<{ method: string }> = (yield* fs.readFileString(requestLog))
          .trim()
          .split("\n")
          .map((line) => JSON.parse(line));
        expect(requests.some((request) => request.method === "session/new")).toBe(false);
      }).pipe(Effect.scoped),
  );
});

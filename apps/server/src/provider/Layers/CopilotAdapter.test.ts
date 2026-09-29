import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  ApprovalRequestId,
  CopilotSettings,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { ServerConfig } from "../../config.ts";
import { execScriptSource, writeFakeCli } from "../../testUtils/fakeCli.ts";
import { makeCopilotAdapter } from "./CopilotAdapter.ts";

const instanceId = ProviderInstanceId.make("copilot-test");
const threadId = ThreadId.make("copilot-thread");
const decodeSettings = Schema.decodeSync(CopilotSettings);
const decodeLog = Schema.decodeEffect(
  Schema.Array(Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown))),
);

const makeHarness = Effect.fn("makeCopilotAdapterTestHarness")(function* (
  environment: Record<string, string> = {},
  onAuthRequired?: Effect.Effect<void>,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-copilot-adapter-" });
  const requestLog = path.join(cwd, "requests.ndjson");
  yield* fs.writeFileString(requestLog, "");
  const scriptPath = yield* path.fromFileUrl(
    new URL("../../../scripts/copilot-mock-agent.ts", import.meta.url),
  );
  const binaryPath = yield* Effect.sync(() =>
    writeFakeCli({
      directory: cwd,
      name: "copilot",
      source: execScriptSource({ scriptPath, expectedArgs: ["--acp", "--stdio"] }),
    }),
  );
  const adapter = yield* makeCopilotAdapter(decodeSettings({ enabled: true, binaryPath }), {
    instanceId,
    environment: { ...process.env, ...environment, T3_COPILOT_REQUEST_LOG: requestLog },
    ...(onAuthRequired ? { onAuthRequired } : {}),
  });
  const events = yield* Queue.unbounded<ProviderRuntimeEvent>();
  const seen: ProviderRuntimeEvent[] = [];
  yield* adapter.streamEvents.pipe(
    Stream.runForEach((event) =>
      Effect.sync(() => seen.push(event)).pipe(Effect.andThen(Queue.offer(events, event))),
    ),
    Effect.forkScoped({ startImmediately: true }),
  );
  const waitFor = <T extends ProviderRuntimeEvent["type"]>(type: T) =>
    Effect.gen(function* () {
      while (true) {
        const event = yield* Queue.take(events);
        if (event.type === type) return event as Extract<ProviderRuntimeEvent, { type: T }>;
      }
    });
  const readLog = fs
    .readFileString(requestLog)
    .pipe(Effect.flatMap((text) => decodeLog(text.split("\n").filter((line) => line.trim()))));
  const output = () =>
    seen.flatMap((event) => (event.type === "content.delta" ? [event.payload.delta] : [])).join("");
  return { adapter, cwd, seen, waitFor, readLog, output };
});

const layer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-copilot-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

it.layer(layer)("CopilotAdapter", (it) => {
  it.effect("streams consecutive turns and applies model, reasoning, and plan changes", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      yield* h.adapter.startSession({ threadId, cwd: h.cwd, runtimeMode: "approval-required" });
      const first = yield* h.adapter.sendTurn({ threadId, input: "First turn" });
      yield* h.waitFor("turn.completed");
      const second = yield* h.adapter.sendTurn({
        threadId,
        input: "Second turn",
        interactionMode: "plan",
        modelSelection: {
          instanceId,
          model: "claude-sonnet-4.6",
          options: [{ id: "reasoning_effort", value: "high" }],
        },
      });
      yield* h.waitFor("turn.completed");
      expect(second.turnId).not.toBe(first.turnId);
      expect(h.output()).toBe("reply:1:gpt-5.4reply:2:claude-sonnet-4.6");
      const requests = yield* h.readLog;
      expect(
        requests
          .filter((request) => request.method === "authenticate")
          .map((request) => request.params),
      ).toEqual([{ methodId: "copilot-login" }]);
      const config = requests
        .filter((request) => request.method === "session/set_config_option")
        .map((request) => request.params);
      expect(config).toContainEqual({
        sessionId: "copilot-native-session",
        configId: "reasoning_effort",
        value: "high",
      });
      expect(config).toContainEqual({
        sessionId: "copilot-native-session",
        configId: "mode",
        value: "https://agentclientprotocol.com/protocol/session-modes#plan",
      });
      expect((yield* h.adapter.readThread(threadId)).turns).toHaveLength(2);
    }),
  );

  for (const [decision, nativeOption] of [
    ["acceptForSession", "native:session/opaque"],
    ["decline", "native:deny/opaque"],
  ] as const) {
    it.effect(`returns the native permission option for ${decision}`, () =>
      Effect.gen(function* () {
        const h = yield* makeHarness({ T3_COPILOT_PERMISSION: "1" });
        yield* h.adapter.startSession({ threadId, cwd: h.cwd, runtimeMode: "approval-required" });
        const turn = yield* h.adapter
          .sendTurn({ threadId, input: "Run a command" })
          .pipe(Effect.forkScoped({ startImmediately: true }));
        const opened = yield* h.waitFor("request.opened");
        expect(opened.payload.options?.map((option) => option.decision)).toContain(
          "acceptForSession",
        );
        yield* h.adapter.respondToRequest(
          threadId,
          ApprovalRequestId.make(opened.requestId!),
          decision,
        );
        yield* Fiber.join(turn);
        yield* h.waitFor("turn.completed");
        expect(h.output()).toBe(`permission:${nativeOption}`);
        expect(
          h.seen.filter(
            (event) => event.type === "request.resolved" && event.requestId === opened.requestId,
          ),
        ).toHaveLength(1);
      }),
    );
  }

  it.effect("approves a remaining full-access request with the offered one-time option", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness({ T3_COPILOT_PERMISSION: "1", T3_COPILOT_OMIT_ALWAYS: "1" });
      yield* h.adapter.startSession({ threadId, cwd: h.cwd, runtimeMode: "full-access" });
      yield* h.adapter.sendTurn({ threadId, input: "Run a command" });
      yield* h.waitFor("turn.completed");
      expect(h.output()).toBe("permission:native:once/opaque");
      expect(h.seen.some((event) => event.type === "request.opened")).toBe(false);
      expect(
        (yield* h.readLog)
          .filter((request) => request.method === "session/set_config_option")
          .map((request) => request.params),
      ).toContainEqual({
        sessionId: "copilot-native-session",
        configId: "allow_all",
        value: "on",
      });
    }),
  );

  it.effect("interrupts a pending approval exactly once and accepts the next turn", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness({ T3_COPILOT_PERMISSION: "1" });
      yield* h.adapter.startSession({ threadId, cwd: h.cwd, runtimeMode: "approval-required" });
      const first = yield* h.adapter
        .sendTurn({ threadId, input: "Run a command" })
        .pipe(Effect.forkScoped({ startImmediately: true }));
      const started = yield* h.waitFor("turn.started");
      const opened = yield* h.waitFor("request.opened");
      yield* h.adapter.interruptTurn(threadId, started.turnId);
      yield* Fiber.join(first);
      yield* h.waitFor("turn.completed");
      yield* h.adapter.sendTurn({ threadId, input: "Continue after cancellation" });
      yield* h.waitFor("turn.completed");
      expect(
        h.seen.filter(
          (event) => event.type === "request.resolved" && event.requestId === opened.requestId,
        ),
      ).toHaveLength(1);
      expect(
        h.seen.filter(
          (event) => event.type === "turn.completed" && event.turnId === started.turnId,
        ),
      ).toHaveLength(1);
      expect(h.output()).toBe("reply:2:gpt-5.4");
      const responses = (yield* h.readLog).filter((entry) => "result" in entry);
      expect(responses).toHaveLength(1);
      expect(responses[0]?.result).toEqual({ outcome: { outcome: "cancelled" } });
    }),
  );

  it.effect(
    "loads a saved session without replaying history and preserves native slash commands",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const original = yield* h.adapter.startSession({
          threadId,
          cwd: h.cwd,
          runtimeMode: "full-access",
        });
        yield* h.adapter.sendTurn({ threadId, input: "Remember this conversation" });
        yield* h.waitFor("turn.completed");
        yield* h.adapter.stopSession(threadId);
        yield* h.adapter.startSession({
          threadId,
          cwd: h.cwd,
          runtimeMode: "full-access",
          resumeCursor: original.resumeCursor,
        });
        yield* h.adapter.sendTurn({ threadId, input: "/compact" });
        yield* h.waitFor("turn.completed");
        expect(h.output()).not.toContain("OLD REPLAY");
        expect(
          h.seen.some((event) => event.type === "item.started" && event.itemId === "old-tool"),
        ).toBe(false);
        const requests = yield* h.readLog;
        expect(requests.filter((request) => request.method === "session/load")).toHaveLength(1);
        expect(requests.some((request) => request.method === "session/resume")).toBe(false);
        expect(
          requests.findLast((request) => request.method === "session/prompt")?.params,
        ).toMatchObject({ prompt: [{ type: "text", text: "/compact" }] });
      }),
  );

  it.effect("rejects rollback without losing an existing conversation", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      yield* h.adapter.startSession({ threadId, cwd: h.cwd, runtimeMode: "full-access" });
      yield* h.adapter.sendTurn({ threadId, input: "Remember this turn" });
      yield* h.waitFor("turn.completed");
      const before = yield* h.adapter.readThread(threadId);
      const error = yield* h.adapter.rollbackThread(threadId, 1).pipe(Effect.flip);
      expect(error._tag).toBe("ProviderAdapterRequestError");
      expect((yield* h.adapter.readThread(threadId)).turns).toEqual(before.turns);
      yield* h.adapter.sendTurn({ threadId, input: "Continue the same conversation" });
      yield* h.waitFor("turn.completed");
      expect((yield* h.adapter.readThread(threadId)).turns).toHaveLength(2);
      expect((yield* h.readLog).filter((request) => request.method === "session/new")).toHaveLength(
        1,
      );
    }),
  );

  it.effect("reports authentication expiring during a later prompt", () =>
    Effect.gen(function* () {
      let authFailures = 0;
      const h = yield* makeHarness(
        { T3_COPILOT_PROMPT_AUTH_ERROR: "2" },
        Effect.sync(() => {
          authFailures += 1;
        }),
      );
      yield* h.adapter.startSession({ threadId, cwd: h.cwd, runtimeMode: "approval-required" });
      yield* h.adapter.sendTurn({ threadId, input: "First turn" });
      yield* h.waitFor("turn.completed");
      const error = yield* h.adapter.sendTurn({ threadId, input: "Second turn" }).pipe(Effect.flip);
      expect(error._tag).toBe("ProviderAdapterRequestError");
      expect(authFailures).toBe(1);
      expect((yield* h.waitFor("turn.completed")).payload.state).toBe("failed");
    }),
  );

  it.effect(
    "rejects foreign model selections and unsupported structured answers without sending a turn",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        yield* h.adapter.startSession({ threadId, cwd: h.cwd, runtimeMode: "approval-required" });
        const selectionError = yield* h.adapter
          .sendTurn({
            threadId,
            input: "Do not dispatch",
            modelSelection: {
              instanceId: ProviderInstanceId.make("another-copilot"),
              model: "gpt-5.4",
            },
          })
          .pipe(Effect.flip);
        expect(selectionError._tag).toBe("ProviderAdapterValidationError");
        const inputError = yield* h.adapter
          .respondToUserInput(threadId, ApprovalRequestId.make("unsupported-question"), {
            question: "answer",
          })
          .pipe(Effect.flip);
        expect(inputError._tag).toBe("ProviderAdapterRequestError");
        expect(inputError.message).toMatch(/not support|unsupported/i);
        expect((yield* h.readLog).some((request) => request.method === "session/prompt")).toBe(
          false,
        );
        expect((yield* h.adapter.readThread(threadId)).turns).toHaveLength(0);
      }),
  );
});

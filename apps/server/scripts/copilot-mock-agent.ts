#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";

import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as AcpAgent from "effect-acp/agent";
import * as AcpErrors from "effect-acp/errors";
import type * as Acp from "effect-acp/schema";

const sessionId = "copilot-native-session";
const modePrefix = "https://agentclientprotocol.com/protocol/session-modes#";
const requestLog = process.env.T3_COPILOT_REQUEST_LOG;
let model = "gpt-5.4";
let mode = `${modePrefix}agent`;
let reasoning = "medium";
let allowAll = "off";
let promptCount = 0;

function configOptions(): ReadonlyArray<Acp.SessionConfigOption> {
  return [
    {
      id: "model",
      name: "Model",
      category: "model",
      type: "select",
      currentValue: model,
      options: [
        { value: "gpt-5.4", name: "GPT-5.4" },
        { value: "claude-sonnet-4.6", name: "Claude Sonnet 4.6" },
      ],
    },
    {
      id: "mode",
      name: "Mode",
      category: "mode",
      type: "select",
      currentValue: mode,
      options: ["agent", "plan", "autopilot"].map((name) => ({
        value: `${modePrefix}${name}`,
        name,
      })),
    },
    {
      id: "reasoning_effort",
      name: "Reasoning",
      category: "thought_level",
      type: "select",
      currentValue: reasoning,
      options: ["low", "medium", "high"].map((value) => ({ value, name: value })),
    },
    {
      id: "allow_all",
      name: "Allow All",
      category: "permissions",
      type: "select",
      currentValue: allowAll,
      options: [
        { value: "on", name: "On" },
        { value: "off", name: "Off" },
      ],
    },
  ];
}

function sessionState() {
  return {
    configOptions: configOptions(),
    models: {
      currentModelId: model,
      availableModels: [
        { modelId: "gpt-5.4", name: "GPT-5.4" },
        { modelId: "claude-sonnet-4.6", name: "Claude Sonnet 4.6" },
      ],
    },
    modes: {
      currentModeId: mode,
      availableModes: ["agent", "plan", "autopilot"].map((name) => ({
        id: `${modePrefix}${name}`,
        name,
      })),
    },
  };
}

const program = Effect.gen(function* () {
  const agent = yield* AcpAgent.AcpAgent;
  const cancelled = yield* Deferred.make<void>();
  const message = (text: string) =>
    agent.client.sessionUpdate({
      sessionId,
      update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text } },
    });
  const publishCommands = agent.client.sessionUpdate({
    sessionId,
    update: {
      sessionUpdate: "available_commands_update",
      availableCommands: [{ name: "compact", description: "Compact the conversation" }],
    },
  });

  yield* agent.handleInitialize(() =>
    Effect.succeed({
      protocolVersion: 1,
      agentInfo: { name: "Copilot", version: "1.0.83-test" },
      agentCapabilities: {
        loadSession: true,
        promptCapabilities: { image: true, embeddedContext: true },
        mcpCapabilities: { http: true, sse: true },
        sessionCapabilities: { list: {} },
      },
      authMethods: [{ id: "copilot-login", name: "Log in with Copilot CLI" }],
    }),
  );
  yield* agent.handleAuthenticate(() =>
    process.env.T3_COPILOT_FAIL_AUTH === "1"
      ? Effect.fail(AcpErrors.AcpRequestError.authRequired())
      : Effect.succeed({}),
  );
  yield* agent.handleCreateSession(() =>
    publishCommands.pipe(Effect.as({ sessionId, ...sessionState() })),
  );
  yield* agent.handleLoadSession(() =>
    Effect.gen(function* () {
      yield* message("OLD REPLAY MUST NOT APPEAR");
      yield* agent.client.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: "tool_call",
          toolCallId: "old-tool",
          title: "Old command",
          kind: "execute",
          status: "completed",
        },
      });
      yield* publishCommands;
      return sessionState();
    }),
  );
  yield* agent.handleSetSessionConfigOption((request) =>
    Effect.gen(function* () {
      const option = configOptions().find((entry) => entry.id === request.configId);
      if (
        typeof request.value !== "string" ||
        option?.type !== "select" ||
        !option.options.some((entry) => "value" in entry && entry.value === request.value)
      ) {
        return yield* AcpErrors.AcpRequestError.invalidParams("Unknown configuration value");
      }
      switch (request.configId) {
        case "model":
          model = request.value;
          break;
        case "mode":
          mode = request.value;
          allowAll = "off";
          break;
        case "reasoning_effort":
          reasoning = request.value;
          break;
        case "allow_all":
          allowAll = request.value;
          break;
      }
      return { configOptions: configOptions() };
    }),
  );
  yield* agent.handleCancel(() => Deferred.succeed(cancelled, undefined).pipe(Effect.asVoid));
  yield* agent.handlePrompt(() =>
    Effect.gen(function* () {
      promptCount += 1;
      if (process.env.T3_COPILOT_PROMPT_AUTH_ERROR === String(promptCount)) {
        return yield* AcpErrors.AcpRequestError.authRequired();
      }
      if (process.env.T3_COPILOT_PERMISSION === "1" && promptCount === 1) {
        yield* agent.client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: "tool_call",
            toolCallId: "command-1",
            title: "Run command",
            kind: "execute",
            status: "pending",
            rawInput: { command: "echo approved" },
          },
        });
        const response = yield* agent.client.requestPermission({
          sessionId,
          toolCall: {
            toolCallId: "command-1",
            title: "Run command",
            kind: "execute",
            status: "pending",
            rawInput: { command: "echo approved" },
          },
          options: [
            { optionId: "native:once/opaque", kind: "allow_once", name: "Allow once" },
            ...(process.env.T3_COPILOT_OMIT_ALWAYS === "1"
              ? []
              : [
                  {
                    optionId: "native:session/opaque",
                    kind: "allow_always" as const,
                    name: "Always allow",
                  },
                ]),
            { optionId: "native:deny/opaque", kind: "reject_once", name: "Deny" },
          ],
        });
        if (response.outcome.outcome === "cancelled") {
          yield* Deferred.await(cancelled);
          return { stopReason: "cancelled" as const };
        }
        yield* agent.client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: "tool_call_update",
            toolCallId: "command-1",
            status: response.outcome.optionId === "native:deny/opaque" ? "failed" : "completed",
          },
        });
        yield* message(`permission:${response.outcome.optionId}`);
      } else {
        yield* message(`reply:${promptCount}:`);
        yield* message(model);
      }
      return { stopReason: "end_turn" as const };
    }),
  );
  return yield* Effect.never;
}).pipe(
  Effect.provide(
    AcpAgent.layerStdio({
      logIncoming: true,
      logger: (event) =>
        event.direction === "incoming" &&
        event.stage === "raw" &&
        typeof event.payload === "string" &&
        requestLog
          ? Effect.sync(() => NodeFS.appendFileSync(requestLog, `${event.payload}\n`, "utf8"))
          : Effect.void,
    }),
  ),
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program);

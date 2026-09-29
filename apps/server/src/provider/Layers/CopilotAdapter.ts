import {
  ApprovalRequestId,
  EventId,
  ProviderDriverKind,
  RuntimeRequestId,
  TurnId,
  type CopilotSettings,
  type ProviderApprovalDecision,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ThreadId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as Acp from "effect-acp/schema";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import { buildRuntimeInstructions } from "../RuntimeInstructions.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { mapAcpToAdapterError } from "../acp/AcpAdapterSupport.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpPlanUpdatedEvent,
  makeAcpRequestOpenedEvent,
  makeAcpRequestResolvedEvent,
  makeAcpToolCallEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import { makeAcpNativeLoggerFactory } from "../acp/AcpNativeLogging.ts";
import { parsePermissionRequest } from "../acp/AcpRuntimeModel.ts";
import type * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import {
  applyCopilotSessionConfiguration,
  copilotApprovalOptions,
  makeCopilotAcpRuntime,
  selectCopilotPermissionOption,
} from "../acp/CopilotAcpSupport.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import type { EventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = ProviderDriverKind.make("githubCopilot");
const ResumeCursor = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  sessionId: Schema.NonEmptyString,
});
const decodeResumeCursor = Schema.decodeUnknownOption(ResumeCursor);
const isAcpError = Schema.is(EffectAcpErrors.AcpError);

type Adapter = ProviderAdapterShape<ProviderAdapterError>;
type Runtime = AcpSessionRuntime.AcpSessionRuntime["Service"];
type NativePermission = Acp.RequestPermissionRequest;
type NativePermissionResponse = Acp.RequestPermissionResponse;

interface PendingApproval {
  readonly request: NativePermission;
  readonly result: Deferred.Deferred<{
    readonly decision: ProviderApprovalDecision;
    readonly response: NativePermissionResponse;
  }>;
}

interface TurnIntent {
  readonly id: TurnId;
  readonly generation: number;
  settled: boolean;
}

interface SessionContext {
  readonly threadId: ThreadId;
  readonly nativeSessionId: string;
  readonly cwd: string;
  readonly scope: Scope.Closeable;
  readonly runtime: Runtime;
  readonly promptLock: Semaphore.Semaphore;
  readonly stopLock: Semaphore.Semaphore;
  readonly approvals: Map<ApprovalRequestId, PendingApproval>;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  session: ProviderSession;
  activeTurnId: TurnId | undefined;
  promptFiber: Fiber.Fiber<Acp.PromptResponse, EffectAcpErrors.AcpError> | undefined;
  generation: number;
  stopped: boolean;
  closed: boolean;
  disconnected: boolean;
}

export interface CopilotAdapterOptions {
  readonly instanceId: import("@t3tools/contracts").ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly onSessionStarted?: (
    started: AcpSessionRuntime.AcpSessionRuntimeStartResult,
    cwd: string,
  ) => Effect.Effect<void>;
  readonly onAvailableCommands?: (
    commands: ReadonlyArray<Acp.AvailableCommand>,
    cwd: string,
  ) => Effect.Effect<void>;
  readonly onConfigOptionsUpdated?: (
    configOptions: ReadonlyArray<Acp.SessionConfigOption>,
  ) => Effect.Effect<void>;
  readonly onAuthRequired?: Effect.Effect<void>;
}

/** Copilot owns the agent loop; T3 owns the durable thread, approvals, and UI events. */
export const makeCopilotAdapter = Effect.fn("makeCopilotAdapter")(function* (
  settings: CopilotSettings,
  options: CopilotAdapterOptions,
) {
  const crypto = yield* Crypto.Crypto;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const serverConfig = yield* ServerConfig;
  const ownerScope = yield* Effect.scope;
  const makeNativeLoggers = yield* makeAcpNativeLoggerFactory();
  const sessions = new Map<ThreadId, SessionContext>();
  const locks = yield* SynchronizedRef.make(new Map<ThreadId, Semaphore.Semaphore>());
  const eventBus = yield* PubSub.unbounded<ProviderRuntimeEvent>();
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const randomId = crypto.randomUUIDv4.pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "crypto/randomUUIDv4",
          detail: "Could not create a Copilot event ID.",
          cause,
        }),
    ),
  );
  const stamp = Effect.all({ eventId: Effect.map(randomId, EventId.make), createdAt: nowIso });
  const emit = (event: ProviderRuntimeEvent) => PubSub.publish(eventBus, event).pipe(Effect.asVoid);

  const withThreadLock = <A, E, R>(threadId: ThreadId, task: Effect.Effect<A, E, R>) =>
    SynchronizedRef.modifyEffect(locks, (current) => {
      const existing = current.get(threadId);
      if (existing) return Effect.succeed([existing, current] as const);
      return Semaphore.make(1).pipe(
        Effect.map((lock) => [lock, new Map(current).set(threadId, lock)] as const),
      );
    }).pipe(Effect.flatMap((lock) => lock.withPermit(task)));

  const requireSession = (threadId: ThreadId) => {
    const context = sessions.get(threadId);
    return context && !context.stopped
      ? Effect.succeed(context)
      : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
  };

  const cancelApprovals = Effect.fn("CopilotAdapter.cancelApprovals")(function* (
    context: SessionContext,
  ) {
    for (const pending of context.approvals.values()) {
      yield* Deferred.succeed(pending.result, {
        decision: "cancel",
        response: { outcome: { outcome: "cancelled" } },
      });
    }
  });

  const stopContext = (context: SessionContext) =>
    context.stopLock
      .withPermit(
        Effect.gen(function* () {
          if (context.closed) return;
          context.stopped = true;
          yield* Effect.gen(function* () {
            yield* cancelApprovals(context);
            if (context.promptFiber && !context.disconnected)
              yield* Effect.ignore(context.runtime.cancel);
          }).pipe(Effect.ensuring(Scope.close(context.scope, Exit.void)));
          context.closed = true;
          if (sessions.get(context.threadId) === context) sessions.delete(context.threadId);
          yield* emit({
            type: "session.exited",
            ...(yield* stamp),
            provider: PROVIDER,
            threadId: context.threadId,
            payload: {
              exitKind: context.disconnected ? "error" : "graceful",
              ...(context.disconnected ? { reason: "Copilot ACP process stopped." } : {}),
            },
          });
        }),
      )
      .pipe(Effect.uninterruptible);

  const handlePermission = Effect.fn("CopilotAdapter.handlePermission")(function* (
    context: SessionContext,
    request: NativePermission,
  ): Effect.fn.Return<NativePermissionResponse, ProviderAdapterError> {
    if (context.stopped || request.sessionId !== context.nativeSessionId) {
      return { outcome: { outcome: "cancelled" } };
    }
    // A native full-access session should not normally ask. If it does, grant
    // only this request; managed policy can still require a visible choice.
    if (context.session.runtimeMode === "full-access") {
      const id = selectCopilotPermissionOption(request, "accept");
      if (id) return { outcome: { outcome: "selected", optionId: id } };
    }
    if (context.session.runtimeMode === "auto-accept-edits" && request.toolCall?.kind === "edit") {
      const id = selectCopilotPermissionOption(request, "accept");
      if (id) return { outcome: { outcome: "selected", optionId: id } };
    }
    const permission = parsePermissionRequest(request);
    const requestId = ApprovalRequestId.make(yield* randomId);
    const result = yield* Deferred.make<{
      readonly decision: ProviderApprovalDecision;
      readonly response: NativePermissionResponse;
    }>();
    context.approvals.set(requestId, { request, result });
    try {
      yield* emit(
        makeAcpRequestOpenedEvent({
          stamp: yield* stamp,
          provider: PROVIDER,
          threadId: context.threadId,
          turnId: context.activeTurnId,
          requestId: RuntimeRequestId.make(requestId),
          permissionRequest: permission,
          approvalOptions: copilotApprovalOptions(request),
          detail: permission.detail ?? request.toolCall?.title ?? "Copilot requests permission.",
          args: request,
          source: "acp.jsonrpc",
          method: "session/request_permission",
          rawPayload: request,
        }),
      );
      const resolved = yield* Deferred.await(result);
      yield* emit(
        makeAcpRequestResolvedEvent({
          stamp: yield* stamp,
          provider: PROVIDER,
          threadId: context.threadId,
          turnId: context.activeTurnId,
          requestId: RuntimeRequestId.make(requestId),
          permissionRequest: permission,
          decision: resolved.decision,
        }),
      );
      return resolved.response;
    } finally {
      context.approvals.delete(requestId);
    }
  });

  const handleEvent = Effect.fn("CopilotAdapter.handleEvent")(function* (
    context: SessionContext,
    event: AcpSessionRuntime.AcpSessionRuntimeEvent,
  ) {
    switch (event._tag) {
      case "EventStreamBarrier":
        yield* Deferred.succeed(event.acknowledge, undefined);
        return;
      case "ConnectionTerminated":
        context.disconnected = true;
        // Close from the owner scope: closing the session scope inside its own
        // notification fiber would wait for itself to terminate.
        yield* stopContext(context).pipe(Effect.forkIn(ownerScope));
        return;
      case "AvailableCommandsUpdated":
        yield* options.onAvailableCommands?.(event.availableCommands, context.cwd) ?? Effect.void;
        return;
      case "ConfigOptionsUpdated":
        yield* options.onConfigOptionsUpdated?.(event.configOptions) ?? Effect.void;
        return;
      case "ModeChanged":
        return;
    }
    // session/load replays old notifications. T3 already has the durable
    // versions, so only current-turn activity enters the event log.
    if (!context.activeTurnId || context.stopped) return;
    switch (event._tag) {
      case "AssistantItemStarted":
      case "AssistantItemCompleted":
        yield* emit(
          makeAcpAssistantItemEvent({
            stamp: yield* stamp,
            provider: PROVIDER,
            threadId: context.threadId,
            turnId: context.activeTurnId,
            itemId: event.itemId,
            lifecycle: event._tag === "AssistantItemStarted" ? "item.started" : "item.completed",
          }),
        );
        return;
      case "PlanUpdated":
        yield* emit(
          makeAcpPlanUpdatedEvent({
            stamp: yield* stamp,
            provider: PROVIDER,
            threadId: context.threadId,
            turnId: context.activeTurnId,
            payload: event.payload,
            source: "acp.jsonrpc",
            method: "session/update",
            rawPayload: event.rawPayload,
          }),
        );
        return;
      case "ToolCallUpdated":
        yield* emit(
          makeAcpToolCallEvent({
            stamp: yield* stamp,
            provider: PROVIDER,
            threadId: context.threadId,
            turnId: context.activeTurnId,
            toolCall: event.toolCall,
            rawPayload: event.rawPayload,
          }),
        );
        return;
      case "ThoughtDelta":
        yield* emit(
          makeAcpContentDeltaEvent({
            stamp: yield* stamp,
            provider: PROVIDER,
            threadId: context.threadId,
            turnId: context.activeTurnId,
            streamKind: "reasoning_text",
            text: event.text,
            rawPayload: event.rawPayload,
          }),
        );
        return;
      case "ContentDelta":
        yield* emit(
          makeAcpContentDeltaEvent({
            stamp: yield* stamp,
            provider: PROVIDER,
            threadId: context.threadId,
            turnId: context.activeTurnId,
            ...(event.itemId ? { itemId: event.itemId } : {}),
            text: event.text,
            rawPayload: event.rawPayload,
          }),
        );
        return;
    }
  });

  const startSession: Adapter["startSession"] = (input) =>
    withThreadLock(
      input.threadId,
      Effect.gen(function* () {
        if (!settings.enabled)
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "Enable GitHub Copilot in provider settings before starting a thread.",
          });
        if (input.provider && input.provider !== PROVIDER)
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "Provider does not match GitHub Copilot.",
          });
        if (input.providerInstanceId && input.providerInstanceId !== options.instanceId)
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "Provider instance does not match GitHub Copilot.",
          });
        if (input.modelSelection && input.modelSelection.instanceId !== options.instanceId)
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "The selected model belongs to another provider instance.",
          });
        if (!input.cwd?.trim())
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "cwd is required.",
          });
        const cursor =
          input.resumeCursor === undefined ? Option.none() : decodeResumeCursor(input.resumeCursor);
        if (input.resumeCursor !== undefined && Option.isNone(cursor))
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "The saved Copilot session is invalid. Start a new thread.",
          });
        const previous = sessions.get(input.threadId);
        if (previous) yield* stopContext(previous);
        const cwd = path.resolve(input.cwd);
        const sessionScope = yield* Scope.make("sequential");
        let transferred = false;
        let context: SessionContext | undefined;
        yield* Effect.addFinalizer(() =>
          transferred ? Effect.void : Scope.close(sessionScope, Exit.void),
        );
        return yield* Effect.gen(function* () {
          const mcp = McpProviderSession.readMcpProviderSession(input.threadId);
          const runtime = yield* makeCopilotAcpRuntime({
            copilotSettings: settings,
            environment: McpProviderSession.withAgentDeviceEnvironment(
              options.environment ?? process.env,
              mcp,
            ),
            childProcessSpawner: spawner,
            cwd,
            clientInfo: { name: "t3-code", version: "0.0.0" },
            ...(Option.isSome(cursor) ? { resumeSessionId: cursor.value.sessionId } : {}),
            mcpServers: mcp
              ? [
                  {
                    type: "http",
                    name: "t3-code",
                    url: mcp.endpoint,
                    headers: [{ name: "Authorization", value: mcp.authorizationHeader }],
                  },
                ]
              : [],
            ...makeNativeLoggers({
              nativeEventLogger: options.nativeEventLogger,
              provider: PROVIDER,
              threadId: input.threadId,
            }),
          }).pipe(Effect.provideService(Crypto.Crypto, crypto));
          yield* runtime.handleRequestPermission((request) =>
            context
              ? handlePermission(context, request).pipe(
                  Effect.mapError((cause) =>
                    EffectAcpErrors.AcpRequestError.internalError(
                      "Could not process a GitHub Copilot permission request.",
                      undefined,
                      { cause },
                    ),
                  ),
                )
              : Effect.succeed({ outcome: { outcome: "cancelled" } } as NativePermissionResponse),
          );
          const started = yield* runtime.start();
          const model = yield* applyCopilotSessionConfiguration({
            runtime,
            runtimeMode: input.runtimeMode,
            model: input.modelSelection?.model,
            selections: input.modelSelection?.options ?? undefined,
          });
          yield* options.onSessionStarted?.(started, cwd) ?? Effect.void;
          const createdAt = yield* nowIso;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: options.instanceId,
            threadId: input.threadId,
            cwd,
            status: "ready",
            runtimeMode: input.runtimeMode,
            ...(model ? { model } : {}),
            resumeCursor: { schemaVersion: 1, sessionId: started.sessionId },
            createdAt,
            updatedAt: createdAt,
          };
          context = {
            threadId: input.threadId,
            nativeSessionId: started.sessionId,
            cwd,
            scope: sessionScope,
            runtime,
            promptLock: yield* Semaphore.make(1),
            stopLock: yield* Semaphore.make(1),
            approvals: new Map(),
            turns: [],
            session,
            activeTurnId: undefined,
            promptFiber: undefined,
            generation: 0,
            stopped: false,
            closed: false,
            disconnected: false,
          };
          const running = context;
          sessions.set(input.threadId, running);
          yield* Stream.runForEach(runtime.getEvents(), (event) =>
            handleEvent(running, event),
          ).pipe(
            Effect.catchCause((cause) =>
              Effect.logError("Could not process a Copilot runtime event.", { cause }),
            ),
            Effect.forkIn(sessionScope),
          );
          yield* emit({
            type: "session.started",
            ...(yield* stamp),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { resume: started.initializeResult },
          });
          yield* emit({
            type: "session.state.changed",
            ...(yield* stamp),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { state: "ready", reason: "GitHub Copilot ACP session ready" },
          });
          yield* emit({
            type: "thread.started",
            ...(yield* stamp),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { providerThreadId: started.sessionId },
          });
          yield* runtime.drainEvents;
          if (running.stopped)
            return yield* new ProviderAdapterSessionClosedError({
              provider: PROVIDER,
              threadId: input.threadId,
            });
          transferred = true;
          return session;
        }).pipe(
          Effect.provideService(Scope.Scope, sessionScope),
          Effect.tapError((cause) =>
            isAcpError(cause) && cause._tag === "AcpRequestError" && cause.code === -32000
              ? (options.onAuthRequired ?? Effect.void)
              : Effect.void,
          ),
          Effect.mapError((cause) =>
            isAcpError(cause)
              ? mapAcpToAdapterError(PROVIDER, input.threadId, "session/start", cause)
              : cause,
          ),
        );
      }).pipe(Effect.scoped),
    );

  const sendTurn: Adapter["sendTurn"] = (input) =>
    Effect.gen(function* () {
      const context = yield* requireSession(input.threadId);
      if (input.modelSelection && input.modelSelection.instanceId !== options.instanceId)
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "The selected model belongs to another provider instance.",
        });
      const prompt: Array<Acp.ContentBlock> = [];
      const text = input.input?.trim();
      if (text) prompt.push({ type: "text", text });
      for (const attachment of input.attachments ?? []) {
        if (attachment.type !== "image") continue;
        const attachmentPath = resolveAttachmentPath({
          attachmentsDir: serverConfig.attachmentsDir,
          attachment,
        });
        if (!attachmentPath)
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session/prompt",
            detail: `Invalid attachment id '${attachment.id}'.`,
          });
        const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "session/prompt",
                detail: cause.message,
                cause,
              }),
          ),
        );
        prompt.push({
          type: "image",
          data: Buffer.from(bytes).toString("base64"),
          mimeType: attachment.mimeType,
        });
      }
      if (prompt.length === 0)
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Turn requires text or an image.",
        });
      let intent: TurnIntent | undefined;
      const finishTurn = (
        turn: TurnIntent,
        state: "completed" | "cancelled" | "failed",
        stopReason?: string,
        errorMessage?: string,
      ) =>
        Effect.gen(function* () {
          if (turn.settled || context.stopped || context.generation !== turn.generation) return;
          turn.settled = true;
          context.activeTurnId = undefined;
          context.promptFiber = undefined;
          context.session = {
            ...context.session,
            status: state === "failed" ? "error" : "ready",
            activeTurnId: undefined,
            updatedAt: yield* nowIso,
            lastError: errorMessage,
          };
          yield* emit({
            type: "turn.completed",
            ...(yield* stamp),
            provider: PROVIDER,
            threadId: input.threadId,
            turnId: turn.id,
            payload: {
              state,
              ...(stopReason ? { stopReason } : {}),
              ...(errorMessage ? { errorMessage } : {}),
            },
          });
        }).pipe(Effect.uninterruptible);

      return yield* Effect.gen(function* () {
        const launch = yield* context.promptLock.withPermit(
          Effect.gen(function* () {
            yield* requireSession(input.threadId);
            const turnId = context.activeTurnId ?? TurnId.make(yield* randomId);
            const steering = context.activeTurnId !== undefined;
            const turn: TurnIntent = {
              id: turnId,
              generation: ++context.generation,
              settled: false,
            };
            intent = turn;
            context.activeTurnId = turnId;
            if (!steering)
              yield* emit({
                type: "turn.started",
                ...(yield* stamp),
                provider: PROVIDER,
                threadId: input.threadId,
                turnId,
                payload: { model: input.modelSelection?.model ?? context.session.model ?? "auto" },
              });
            if (context.promptFiber) {
              yield* cancelApprovals(context);
              yield* context.runtime.cancel;
              yield* Fiber.await(context.promptFiber);
            }
            const model = yield* applyCopilotSessionConfiguration({
              runtime: context.runtime,
              runtimeMode: context.session.runtimeMode,
              interactionMode: input.interactionMode,
              model: input.modelSelection?.model ?? context.session.model,
              selections: input.modelSelection?.options ?? undefined,
            });
            context.session = {
              ...context.session,
              status: "running",
              activeTurnId: turnId,
              ...(model ? { model } : {}),
              updatedAt: yield* nowIso,
            };
            const dispatched = yield* Deferred.make<void>();
            const fiber = yield* context.runtime
              .prompt(
                {
                  prompt: /^\/[^\s/]+(?:\s|$)/.test(text ?? "")
                    ? prompt
                    : [
                        ...prompt,
                        {
                          type: "text",
                          text: buildRuntimeInstructions({ harness: "GitHub Copilot", model }),
                        },
                      ],
                },
                { dispatched },
              )
              .pipe(Effect.forkIn(context.scope));
            context.promptFiber = fiber;
            yield* Effect.raceFirst(
              Deferred.await(dispatched),
              Fiber.await(fiber).pipe(
                Effect.flatMap((exit) => exit),
                Effect.asVoid,
              ),
            );
            return { turn, fiber };
          }),
        );
        const result = yield* Fiber.await(launch.fiber).pipe(Effect.flatMap((exit) => exit));
        yield* context.runtime.drainEvents;
        if (context.stopped)
          return yield* new ProviderAdapterSessionClosedError({
            provider: PROVIDER,
            threadId: input.threadId,
          });
        const record = context.turns.find((turn) => turn.id === launch.turn.id);
        if (record) record.items.push(result);
        else context.turns.push({ id: launch.turn.id, items: [result] });
        yield* context.promptLock.withPermit(
          finishTurn(
            launch.turn,
            result.stopReason === "cancelled" ? "cancelled" : "completed",
            result.stopReason,
          ),
        );
        return {
          threadId: input.threadId,
          turnId: launch.turn.id,
          resumeCursor: context.session.resumeCursor,
        };
      }).pipe(
        Effect.tapError((cause) =>
          isAcpError(cause) && cause._tag === "AcpRequestError" && cause.code === -32000
            ? (options.onAuthRequired ?? Effect.void)
            : Effect.void,
        ),
        Effect.mapError((cause) =>
          isAcpError(cause)
            ? mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", cause)
            : cause,
        ),
        Effect.tapError((cause) =>
          Effect.suspend(() =>
            intent
              ? context.promptLock.withPermit(
                  finishTurn(intent, "failed", undefined, cause.message),
                )
              : Effect.void,
          ),
        ),
        Effect.onInterrupt(() =>
          context.promptLock.withPermit(
            Effect.gen(function* () {
              const turn = intent;
              if (
                !turn ||
                turn.settled ||
                context.stopped ||
                context.generation !== turn.generation
              )
                return;
              yield* cancelApprovals(context);
              yield* Effect.ignore(context.runtime.cancel);
              if (context.promptFiber) yield* Fiber.interrupt(context.promptFiber);
              yield* finishTurn(turn, "cancelled", "cancelled");
            }),
          ),
        ),
      );
    });

  const interruptTurn: Adapter["interruptTurn"] = (threadId) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      yield* cancelApprovals(context);
      yield* context.runtime.cancel.pipe(
        Effect.mapError((cause) =>
          mapAcpToAdapterError(PROVIDER, threadId, "session/cancel", cause),
        ),
      );
    });

  const respondToRequest: Adapter["respondToRequest"] = (threadId, requestId, decision) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      const pending = context.approvals.get(requestId);
      if (!pending)
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/request_permission",
          detail: `Unknown pending approval: ${requestId}`,
        });
      const optionId = selectCopilotPermissionOption(pending.request, decision);
      if (decision !== "cancel" && optionId === undefined)
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "respondToRequest",
          issue: "Copilot did not offer that approval choice.",
        });
      yield* Deferred.succeed(pending.result, {
        decision,
        response: optionId
          ? { outcome: { outcome: "selected", optionId } }
          : { outcome: { outcome: "cancelled" } },
      });
    });

  const respondToUserInput: Adapter["respondToUserInput"] = (threadId) =>
    Effect.gen(function* () {
      yield* requireSession(threadId);
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "session/elicitation",
        detail: "Copilot CLI ACP does not support structured questions.",
      });
    });

  const readThread: Adapter["readThread"] = (threadId) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      return { threadId, turns: context.turns };
    });

  const rollbackThread: Adapter["rollbackThread"] = (threadId, numTurns) =>
    Effect.gen(function* () {
      yield* requireSession(threadId);
      if (!Number.isInteger(numTurns) || numTurns < 1)
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "numTurns must be an integer >= 1.",
        });
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "thread/rollback",
        detail: "Copilot ACP does not support conversation rollback.",
      });
    });

  const stopSession: Adapter["stopSession"] = (threadId) =>
    withThreadLock(
      threadId,
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        yield* stopContext(context);
      }),
    );
  const listSessions: Adapter["listSessions"] = () =>
    Effect.sync(() => [...sessions.values()].map((context) => ({ ...context.session })));
  const hasSession: Adapter["hasSession"] = (threadId) =>
    Effect.sync(() => {
      const context = sessions.get(threadId);
      return context !== undefined && !context.stopped;
    });
  const stopAll: Adapter["stopAll"] = () =>
    Effect.forEach([...sessions.values()], stopContext, { discard: true });
  yield* Effect.addFinalizer(() =>
    stopAll().pipe(
      Effect.catch((cause) => Effect.logError("Could not close Copilot sessions.", { cause })),
      Effect.tap(() => PubSub.shutdown(eventBus)),
    ),
  );

  return {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "in-session", supportsConversationRollback: false },
    compaction: { type: "slash-command", command: "/compact" },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
    streamEvents: Stream.fromPubSub(eventBus),
  } satisfies Adapter;
});

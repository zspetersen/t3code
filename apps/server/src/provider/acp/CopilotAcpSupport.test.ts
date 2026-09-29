import { describe, expect, it } from "@effect/vitest";
import { CopilotSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Acp from "effect-acp/schema";
import {
  applyCopilotSessionConfiguration,
  buildCopilotAcpSpawnInput,
  copilotApprovalOptions,
  selectCopilotPermissionOption,
} from "./CopilotAcpSupport.ts";

const decodeSettings = Schema.decodeSync(CopilotSettings);

const request: Acp.RequestPermissionRequest = {
  sessionId: "session",
  toolCall: { toolCallId: "tool", kind: "execute", title: "Run command" },
  options: [
    { optionId: "yes-17", name: "Allow once", kind: "allow_once" },
    { optionId: "remember-42", name: "Always allow", kind: "allow_always" },
    { optionId: "no-19", name: "Deny", kind: "reject_once" },
  ],
};

describe("Copilot ACP boundary", () => {
  it("returns native option IDs and never silently widens a one-time grant", () => {
    expect(selectCopilotPermissionOption(request, "accept")).toBe("yes-17");
    expect(selectCopilotPermissionOption(request, "acceptForSession")).toBe("remember-42");
    expect(selectCopilotPermissionOption(request, "decline")).toBe("no-19");
    expect(selectCopilotPermissionOption(request, "cancel")).toBeUndefined();
    expect(selectCopilotPermissionOption(request, "acceptAlways")).toBeUndefined();
    expect(
      selectCopilotPermissionOption({ ...request, options: request.options.slice(1) }, "accept"),
    ).toBeUndefined();
    expect(copilotApprovalOptions(request)).toEqual([
      { decision: "accept", label: "Allow once" },
      { decision: "acceptForSession", label: "Allow for session" },
      { decision: "decline", label: "Deny" },
    ]);
  });

  it("applies an explicit instance profile and preserves the supplied environment", () => {
    const settings = decodeSettings({ homePath: "/tmp/copilot-profile" });
    const spawn = buildCopilotAcpSpawnInput(settings, "/tmp/project", {
      PATH: "/bin",
      TEST: "yes",
    });
    expect(spawn).toMatchObject({
      command: "copilot",
      cwd: "/tmp/project",
      extendEnv: false,
      env: { PATH: "/bin", TEST: "yes", COPILOT_HOME: "/tmp/copilot-profile" },
    });
    expect(spawn.args).toEqual(["--acp", "--stdio", "--no-auto-update"]);
  });

  it.effect("uses advertised mode IDs and reapplies permissions after changing modes", () =>
    Effect.gen(function* () {
      const changes: Array<[string, string | boolean]> = [];
      const configOptions: Acp.SessionConfigOption[] = [
        {
          id: "mode",
          name: "Mode",
          type: "select",
          currentValue: "https://agentclientprotocol.com/protocol/session-modes#agent",
          options: [
            {
              value: "https://agentclientprotocol.com/protocol/session-modes#agent",
              name: "Agent",
            },
            { value: "https://agentclientprotocol.com/protocol/session-modes#plan", name: "Plan" },
          ],
        },
        {
          id: "allow_all",
          name: "Allow all",
          type: "select",
          currentValue: "on",
          options: [
            { value: "on", name: "On" },
            { value: "off", name: "Off" },
          ],
        },
        {
          id: "model",
          name: "Model",
          type: "select",
          currentValue: "model-a",
          options: [
            { value: "model-a", name: "Model A" },
            { value: "model-b", name: "Model B" },
          ],
        },
      ];
      const runtime = {
        getConfigOptions: Effect.succeed(configOptions),
        setConfigOption: (key: string, value: string | boolean) =>
          Effect.sync(() => {
            changes.push([key, value]);
            return { configOptions };
          }),
        setSessionModel: () => Effect.succeed({}),
      };
      yield* applyCopilotSessionConfiguration({
        runtime,
        runtimeMode: "approval-required",
        interactionMode: "plan",
        model: "model-b",
      });
      expect(changes).toEqual([
        ["model", "model-b"],
        ["mode", "https://agentclientprotocol.com/protocol/session-modes#plan"],
        ["allow_all", "off"],
      ]);
      changes.length = 0;
      yield* applyCopilotSessionConfiguration({
        runtime,
        runtimeMode: "full-access",
        interactionMode: "default",
      });
      expect(changes.at(-1)).toEqual(["allow_all", "on"]);
    }),
  );
});

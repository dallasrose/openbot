// Runs `resources/cli/openbot-cli.mjs` as a child process against the real `AutomationServer` on a
// loopback port. The agent service behind the server is a small in-memory stand-in: a message gets a
// delivery that moves from queued to running to its end each time the CLI reads it. Task-start retry
// checks also drive the real AgentService and SQLite with an injected message-send boundary. No
// provider runs. The script checks
// the CLI's output and exit codes for each outcome and that the token never reaches the output.
// Writes .openbot-build/automation-cli-e2e/report.json. Run: `bun run test:automation-cli`.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  AGENT_PROVIDERS,
  type AgentModelOption,
  type AgentTask,
  DEFAULT_AGENT_DEFAULTS,
  type QueueDelivery,
  type QueuedMessageReceipt,
  type UpdateAgentInput,
} from "@openbot/contracts/ipc";
import { ContextResetBusyError } from "@openbot/contracts/team-protocol/context-reset-v1";
import { Effect } from "effect";
import { AgentSettingsRefused } from "../src/backend/agent/agent-settings-refused";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import { AgentLifecycleFailed, AgentService } from "../src/backend/agent-service";
import { AgentStore } from "../src/backend/agent-store";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import { AutomationServer, type UpdatedAgent } from "../src/main/automation-server";

const OUT = resolve(import.meta.dirname, "../.openbot-build/automation-cli-e2e");
const CLI = resolve(import.meta.dirname, "../resources/cli/openbot-cli.mjs");

interface Check {
  name: string;
  passed: boolean;
  detail: unknown;
}

/** How a message's delivery ends, chosen by the message text. */
type Outcome = "completed" | "failed" | "person" | "forever" | "behind" | "restart";

interface FakeDelivery {
  id: string;
  agentId: string;
  taskId?: string;
  outcome: Outcome;
  reads: number;
}

const agents = [
  {
    id: "agent-ada",
    name: "Ada",
    allowAutomation: true,
    provider: "claude",
    model: "claude-sonnet-5-5",
    reasoningEffort: "high",
  },
  {
    id: "agent-bo",
    name: "Bo",
    allowAutomation: false,
    provider: "claude",
    model: "claude-sonnet-5-5",
    reasoningEffort: "high",
  },
] as const;
function model(
  provider: AgentModelOption["provider"],
  id: string,
  defaultReasoningEffort: AgentModelOption["defaultReasoningEffort"],
  supportedReasoningEfforts: AgentModelOption["supportedReasoningEfforts"],
): AgentModelOption {
  return { provider, id, name: id, description: "", defaultReasoningEffort, supportedReasoningEfforts };
}
const models = [
  model("claude", "claude-sonnet-5-5", "high", ["low", "medium", "high"]),
  model("claude", "claude-opus-5-5", "high", ["low", "medium", "high", "max"]),
  model("codex", "gpt-5.5", "medium", ["low", "medium"]),
];
const hiddenAgents = new Set<string>();
const deliveries = new Map<string, FakeDelivery>();
const tasks = new Map<string, AgentTask>();
const starts = new Map<string, AgentTask>();
const sends = new Map<string, QueuedMessageReceipt>();
const updates: UpdateAgentInput[] = [];
const cleared: string[] = [];
let busy = false;

function deliveryState(
  delivery: FakeDelivery,
): Pick<QueueDelivery, "recipientAgentId" | "status" | "turnId" | "error"> {
  delivery.reads += 1;
  const base = { recipientAgentId: delivery.agentId, turnId: delivery.reads > 1 ? `turn-${delivery.id}` : null };
  if (delivery.reads === 1 || delivery.outcome === "behind") return { ...base, status: "queued", error: null };
  // OpenBot restarts while the CLI waits: the next read comes through a new port and token.
  if (delivery.reads === 2 && delivery.outcome === "restart") restarted = restart();
  if (delivery.reads < 3 || delivery.outcome === "person" || delivery.outcome === "forever")
    return { ...base, status: "running", error: null };
  if (delivery.outcome === "failed") return { ...base, status: "failed", error: "The provider stopped." };
  return { ...base, status: "completed", error: null };
}

const root = mkdtempSync(join(tmpdir(), "openbot-cli-e2e-"));
const options: ConstructorParameters<typeof AutomationServer>[0] = {
  root,
  listAgents: () =>
    agents.map((agent) => ({
      ...agent,
      ...(hiddenAgents.has(agent.id) ? { hiddenAt: new Date().toISOString() } : {}),
    })),
  agentDefaults: () => DEFAULT_AGENT_DEFAULTS,
  listModels: () => models,
  listRoutines: () => [],
  runRoutine: () => Effect.die(new Error("No routine runs in this test.")),
  listTasks: (agentId) => ({
    agentId,
    tasks: [...tasks.values()].filter((task) => task.agentId === agentId),
    limit: 2,
  }),
  readTask: (agentId, taskId) => {
    const task = tasks.get(taskId);
    assert.equal(task?.agentId, agentId);
    return { agentId, threadId: task.threadId, activeTurnId: null, revision: 1, messages: [] };
  },
  startTask: (input) =>
    Effect.gen(function* () {
      const previous = starts.get(input.clientMessageId ?? "");
      if (previous) return previous;
      const id = `task-${tasks.size + 1}`;
      const task: AgentTask = {
        id,
        agentId: input.agentId,
        threadId: `thread-${id}`,
        origin: "user",
        title: input.text,
        state: "idle",
        outcome: null,
        hold: null,
        activeTurnId: null,
        progress: null,
        error: null,
        queuedMessages: [],
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        closedAt: null,
      };
      tasks.set(id, task);
      yield* options.sendMessage({ ...input, taskId: id });
      starts.set(input.clientMessageId ?? "", task);
      return task;
    }),
  deliveryTaskId: (deliveryId) => deliveries.get(deliveryId)?.taskId ?? null,
  sendMessage: (input) => {
    const key = JSON.stringify([input.agentId, input.clientMessageId]);
    const previous = sends.get(key);
    if (previous) return Effect.succeed(previous);
    const id = `delivery-${deliveries.size + 1}`;
    const outcome: Outcome =
      input.text === "please fail"
        ? "failed"
        : input.text === "ask me"
          ? "person"
          : input.text === "forever"
            ? "forever"
            : input.text === "behind a question"
              ? "behind"
              : input.text === "restart"
                ? "restart"
                : "completed";
    deliveries.set(id, {
      id,
      agentId: input.agentId,
      ...(input.taskId ? { taskId: input.taskId } : {}),
      outcome,
      reads: 0,
    });
    const receipt: QueuedMessageReceipt = {
      messageId: `message-${id}`,
      deliveries: [{ id, recipientAgentId: input.agentId, status: "queued", position: 1 }],
    };
    sends.set(key, receipt);
    return Effect.succeed(receipt);
  },
  getLocalAttention: (agentId) => {
    // Only the newest delivery can wait: an earlier question is gone once the next test starts. A
    // delivery that stays queued waits behind an earlier turn's question.
    const newest = [...deliveries.values()].at(-1);
    const waiting = newest?.agentId === agentId && (newest.outcome === "person" || newest.outcome === "behind");
    return {
      prompts: [],
      approvals: [],
      browserTakeovers: waiting ? [{ requestId: "takeover-1", requiresOpenBot: true }] : [],
    };
  },
  respondToLocalAttention: () => Effect.void,
  delivery: (deliveryId) => {
    const delivery = deliveries.get(deliveryId);
    return delivery ? deliveryState(delivery) : null;
  },
  turnAnswer: (_agentId, turnId, taskId) => (taskId ? `Task ${taskId}: ${turnId}` : `Answer of ${turnId}`),
  updateAgent: (input) => {
    if (busy) return Effect.fail({ cause: new AgentSettingsRefused("Wait until the agent is idle.") });
    if (input.providerAccountId === "a0000000000000000") return Effect.fail({ cause: new Error("disk full") });
    updates.push(input);
    const agent: UpdatedAgent = {
      id: input.agentId,
      name: "Ada",
      provider: input.provider ?? "claude",
      model: input.model ?? "claude-sonnet-5-5",
      reasoningEffort: input.reasoningEffort ?? "high",
      ...(input.providerAccountId ? { providerAccountId: input.providerAccountId } : {}),
    };
    return Effect.succeed(agent);
  },
  clearAgentContext: (agentId) => {
    if (busy) return Effect.fail({ cause: new ContextResetBusyError("Wait for the active turn to finish.") });
    cleared.push(agentId);
    return Effect.void;
  },
};
let server = new AutomationServer(options);
let restarted: Promise<void> | null = null;
function restart(): Promise<void> {
  const old = server;
  server = new AutomationServer(options);
  return Effect.runPromise(Effect.andThen(old.stop(), server.sync()));
}

interface Run {
  code: number | null;
  stdout: string;
  stderr: string;
}

function cli(args: string[], stdin?: string): Promise<Run> {
  return new Promise((done, fail) => {
    // The CLI is for Node, which is what users have; Bun runs only this script.
    const child = spawn("node", [CLI, ...args], {
      env: { ...process.env, OPENBOT_AUTOMATION_DIR: root },
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });
    child.on("error", fail);
    child.on("close", (code) => done({ code, stdout, stderr }));
    child.stdin.end(stdin ?? "");
  });
}

mkdirSync(OUT, { recursive: true });
const checks: Check[] = [];
const outputs: string[] = [];
async function check(name: string, args: string[], expectCode: number, test: (run: Run) => boolean, stdin?: string) {
  const run = await cli(args, stdin);
  outputs.push(run.stdout, run.stderr);
  checks.push({ name, passed: run.code === expectCode && test(run), detail: run });
}
const parsed = (run: Run) => JSON.parse(run.stdout);

/** Real task creation and SQLite recovery, with only the provider send boundary replaced. */
async function taskStartChecks() {
  const profile = mkdtempSync(join(tmpdir(), "openbot-task-retry-e2e-"));
  const userData = join(profile, "data");
  const database = new OpenBotDatabase(userData);
  const store = new AgentStore(userData, join(profile, "home"), database);
  let mailbox = new MailboxStore(userData, store.sharedRoot, database);
  const unused = () => Effect.die(new Error("No browser work in this check."));
  const browser: AgentBrowserHost = {
    listTabs: () => [],
    beginTakeover: unused,
    endTakeover: () => {},
    resolveUploadTarget: unused,
    handleDynamicTool: unused,
    userControlTabFor: () => null,
    awaitUserControlRelease: unused,
    onChanged: () => () => {},
    onControlChanged: () => () => {},
    onDocumentChanged: () => () => {},
    clearControls: () => {},
    endControl: () => {},
    close: () => Effect.void,
    clearAgentSession: () => Effect.void,
  };
  // The real start operation owns task creation, concurrent retries and persisted receipt lookup.
  // A message is stored in the real mailbox without starting a provider or a drain.
  class TaskService extends AgentService {
    override readonly sendMessage: AgentService["sendMessage"] = (input, sender) =>
      mailbox
        .enqueue({
          sender: { kind: "user" },
          recipientAgentIds: [input.agentId],
          text: input.text,
          draftIds: [],
          ...(input.taskId ? { taskId: input.taskId } : {}),
          idempotencyKey: `user-send:${createHash("sha256")
            .update(JSON.stringify([sender?.id ?? null, input.agentId, input.clientMessageId]))
            .digest("hex")}`,
        })
        .pipe(Effect.mapError((failure) => new AgentLifecycleFailed({ operation: "test send", cause: failure.cause })));
  }
  const makeService = () => new TaskService({ store, mailbox, browser, offProviders: AGENT_PROVIDERS });
  let service: TaskService | undefined;
  try {
    await Effect.runPromise(store.initialize());
    await Effect.runPromise(mailbox.initialize());
    const agent = await Effect.runPromise(
      store.createAgent({ name: "Ada", description: "Review builds", avatarSeed: "ada", avatarHue: null }),
    );
    service = makeService();
    const input = { agentId: agent.id, text: "Review build", clientMessageId: "retry-start" };
    const [first, retry] = await Effect.runPromise(
      Effect.all([service.startTask(input), service.startTask(input)], { concurrency: "unbounded" }),
    );
    assert.equal(first.id, retry.id);
    assert.equal(service.listTasks(agent.id).tasks.length, 1);
    checks.push({ name: "real task service joins concurrent starts", passed: true, detail: null });
    await Effect.runPromise(service.stop());
    mailbox = new MailboxStore(userData, store.sharedRoot, database);
    await Effect.runPromise(mailbox.initialize());
    service = makeService();
    const recovered = await Effect.runPromise(service.startTask(input));
    assert.equal(recovered.id, first.id);
    assert.equal(service.listTasks(agent.id).tasks.length, 1);
    checks.push({ name: "real task service recovers a start from the persisted mailbox", passed: true, detail: null });
  } finally {
    if (service) await Effect.runPromise(service.stop());
    database.close();
    rmSync(profile, { recursive: true, force: true });
  }
}

try {
  await taskStartChecks();
  await Effect.runPromise(server.sync());
  const token = readFileSync(join(root, "token"), "utf8").trim();

  const api = async (path: string, method = "GET", body?: unknown, headers: Record<string, string> = {}) => {
    const response = await fetch(readFileSync(join(root, "url"), "utf8").trim() + path, {
      method,
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json", ...headers },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    return response.status;
  };
  const taskBody = { text: "Review a build", clientMessageId: "boundary-check" };
  for (const [name, status, expected] of [
    [
      "task create requires bearer authentication",
      await api("/v1/agents/agent-ada/tasks", "POST", taskBody, { authorization: "Bearer invalid" }),
      401,
    ],
    [
      "task create refuses a browser Origin",
      await api("/v1/agents/agent-ada/tasks", "POST", taskBody, { origin: "https://example.test" }),
      403,
    ],
    [
      "task create requires the agent Local scripts grant",
      await api("/v1/agents/agent-bo/tasks", "POST", taskBody),
      403,
    ],
    ["task read requires the agent Local scripts grant", await api("/v1/agents/agent-bo/tasks"), 403],
    [
      "main route refuses a task targeting field",
      await api("/v1/agents/agent-ada/messages", "POST", { ...taskBody, taskId: "missing" }),
      400,
    ],
    [
      "task create requires a retry key",
      await api("/v1/agents/agent-ada/tasks", "POST", { text: "Review a build" }),
      400,
    ],
    [
      "a malformed task path does not queue a main message",
      await api("/v1/agents/agent-ada/tasks//messages", "POST", taskBody),
      404,
    ],
  ] as const) {
    checks.push({ name, passed: status === expected && deliveries.size === 0 && tasks.size === 0, detail: status });
  }
  hiddenAgents.add("agent-ada");
  const hiddenStatus = await api("/v1/agents/agent-ada/tasks", "POST", taskBody);
  checks.push({
    name: "a hidden agent takes no task work",
    passed: hiddenStatus === 409 && tasks.size === 0,
    detail: hiddenStatus,
  });
  hiddenAgents.clear();

  await check("list shows only agents with Local scripts", ["list", "--json"], 0, (run) => {
    const value = parsed(run);
    return value.ok === true && value.agents.length === 1 && value.agents[0].name === "Ada";
  });
  await check("run without --wait returns the queued delivery", ["run", "ada", "hello", "--json"], 0, (run) => {
    const value = parsed(run);
    return value.status === "queued" && value.deliveryId === "delivery-1";
  });
  await check("run --wait prints the turn's answer", ["run", "Ada", "do it", "--wait"], 0, (run) => {
    return run.stdout.trim() === "Answer of turn-delivery-2";
  });
  await check(
    "run --wait --json ends completed with the answer",
    ["run", "agent-ada", "-", "--wait", "--json"],
    0,
    (run) => {
      const value = parsed(run);
      return value.status === "completed" && value.answer === "Answer of turn-delivery-3";
    },
    "a task from stdin",
  );
  await check("a failed turn exits 1", ["run", "Ada", "please fail", "--wait", "--json"], 1, (run) => {
    const value = parsed(run);
    return value.status === "failed" && value.error === "The provider stopped.";
  });
  await check("a turn that waits for a person exits 3", ["run", "Ada", "ask me", "--wait", "--json"], 3, (run) => {
    return parsed(run).state === "needs-person";
  });
  await check(
    "a turn that runs past the timeout exits 4",
    ["run", "Ada", "forever", "--wait", "--timeout", "3s"],
    4,
    (run) => run.stdout.includes("Timed out"),
  );
  await check("status reads a delivery", ["status", "Ada", "delivery-2", "--json"], 0, (run) => {
    return parsed(run).answer === "Answer of turn-delivery-2";
  });
  await check(
    "a task queued behind a question exits 3",
    ["run", "Ada", "behind a question", "--wait", "--json"],
    3,
    (run) => parsed(run).state === "needs-person",
  );
  await check("status of an unknown delivery exits 1", ["status", "Ada", "delivery-404"], 1, (run) => {
    return run.stderr.length > 0;
  });
  const sent = deliveries.size;
  await check(
    "a wrong --timeout exits 2 and sends nothing",
    ["run", "Ada", "hello", "--wait", "--timeout", "soon"],
    2,
    (run) => {
      return run.stderr.length > 0 && deliveries.size === sent;
    },
  );
  await check("an agent without Local scripts is not found", ["run", "Bo", "hello"], 5, (run) => {
    return run.stderr.includes('No agent "Bo"');
  });
  await check("a wrong command line exits 2", ["run", "Ada"], 2, (run) => run.stderr.includes("Usage:"));
  const oldUrl = readFileSync(join(root, "url"), "utf8");
  await check(
    "run --wait continues after OpenBot restarts",
    ["run", "Ada", "restart", "--wait", "--json"],
    0,
    (run) => {
      return parsed(run).status === "completed" && readFileSync(join(root, "url"), "utf8") !== oldUrl;
    },
  );
  await restarted;
  const newToken = readFileSync(join(root, "token"), "utf8").trim();
  await check("set-account changes the account", ["agent", "set-account", "Ada", "a0123456789abcdef"], 0, (run) => {
    return (
      run.stdout.includes("account a0123456789abcdef") && updates.at(-1)?.providerAccountId === "a0123456789abcdef"
    );
  });
  await check(
    "set-model sends the model and the effort",
    ["agent", "set-model", "Ada", "claude-opus-5-5", "--effort", "max", "--json"],
    0,
    (run) => parsed(run).agent.model === "claude-opus-5-5" && updates.at(-1)?.reasoningEffort === "max",
  );
  await check("set-model takes the provider from the model", ["agent", "set-model", "Ada", "gpt-5.5"], 0, () => {
    const update = updates.at(-1);
    // The agent's `high` is not supported by gpt-5.5, so the model's default replaces it.
    return update?.provider === "codex" && update.model === "gpt-5.5" && update.reasoningEffort === "medium";
  });
  const updateCount = updates.length;
  await check(
    "an effort that the model does not support exits 1",
    ["agent", "set-model", "Ada", "claude-sonnet-5-5", "--effort", "max"],
    1,
    (run) => run.stderr.includes("max") && updates.length === updateCount,
  );
  await check(
    "an unexpected update failure exits 1 without its detail",
    ["agent", "set-account", "Ada", "a0000000000000000"],
    1,
    (run) => run.stderr.includes("internal error") && !run.stderr.includes("disk full"),
  );
  await check("an invalid account ID is refused", ["agent", "set-account", "Ada", "../x"], 1, (run) => {
    return run.stderr.includes("providerAccountId is invalid.");
  });
  await check("new-chat starts a new chat", ["agent", "new-chat", "Ada", "--json"], 0, (run) => {
    return parsed(run).cleared === true && cleared.at(-1) === "agent-ada";
  });
  await check("new-chat needs exactly one agent", ["agent", "new-chat"], 2, (run) => run.stderr.includes("Usage:"));
  busy = true;
  await check("a refused update exits 1 with its reason", ["agent", "set-account", "Ada", "default"], 1, (run) => {
    return run.stderr.includes("Wait until the agent is idle.");
  });
  await check("new-chat while the agent works exits 1 with its reason", ["agent", "new-chat", "Ada"], 1, (run) => {
    return run.stderr.includes("Wait for the active turn to finish.") && cleared.length === 1;
  });
  busy = false;
  const beforeTaskDeliveries = deliveries.size;
  await check(
    "--new-task creates one separate task",
    ["run", "Ada", "Review a change", "--new-task", "--id", "start-review", "--json"],
    0,
    (run) => {
      return parsed(run).taskId === "task-1" && tasks.size === 1 && deliveries.size === beforeTaskDeliveries + 1;
    },
  );
  await check(
    "task start retry reuses the task and first delivery",
    ["run", "Ada", "Review a change", "--new-task", "--id", "start-review", "--json"],
    0,
    (run) => {
      return parsed(run).taskId === "task-1" && tasks.size === 1 && deliveries.size === beforeTaskDeliveries + 1;
    },
  );
  await restart();
  await check(
    "task start retry after listener restart reuses the task",
    ["run", "Ada", "Review a change", "--new-task", "--id", "start-review", "--json"],
    0,
    () => {
      return tasks.size === 1 && deliveries.size === beforeTaskDeliveries + 1;
    },
  );
  await check(
    "--task sends to the existing lane and reads its answer",
    ["run", "Ada", "Follow up", "--task", "task-1", "--id", "follow-up", "--wait", "--json"],
    0,
    (run) => {
      const value = parsed(run);
      return (
        value.taskId === "task-1" &&
        value.status === "completed" &&
        value.answer === `Task task-1: turn-${value.deliveryId}`
      );
    },
  );
  const taskDelivery = [...deliveries.values()].at(-1);
  assert.ok(taskDelivery);
  await check(
    "task send retry does not queue another message",
    ["run", "Ada", "Follow up", "--task", "task-1", "--id", "follow-up", "--json"],
    0,
    (run) => parsed(run).deliveryId === taskDelivery.id,
  );
  await check(
    "task list returns the persistent task",
    ["task", "list", "Ada", "--json"],
    0,
    (run) => parsed(run).tasks[0].id === "task-1",
  );
  await check(
    "task read returns only the task conversation",
    ["task", "read", "Ada", "task-1", "--json"],
    0,
    (run) => parsed(run).conversation.threadId === "thread-task-1",
  );
  await check(
    "main chat and task start use separate retry namespaces",
    ["run", "Ada", "Main review", "--id", "start-review", "--json"],
    0,
    (run) => parsed(run).deliveryId !== taskDelivery.id && !parsed(run).taskId,
  );
  await check(
    "main chat and task send use separate retry namespaces",
    ["run", "Ada", "Main follow-up", "--id", "follow-up", "--json"],
    0,
    (run) => parsed(run).deliveryId !== taskDelivery.id && !parsed(run).taskId,
  );
  const count = deliveries.size;
  await check(
    "unknown task is refused without main chat fallback",
    ["run", "Ada", "hello", "--task", "missing", "--json"],
    1,
    () => deliveries.size === count,
  );
  const taskOne = tasks.get("task-1");
  assert.ok(taskOne);
  tasks.set("other-task", { ...taskOne, id: "other-task", agentId: "agent-bo" });
  await check(
    "another agent's task is refused",
    ["run", "Ada", "hello", "--task", "other-task", "--json"],
    1,
    () => deliveries.size === count,
  );
  await check(
    "another task's delivery is refused",
    ["status", "Ada", taskDelivery.id, "--task", "other-task", "--json"],
    1,
    (run) => parsed(run).error === "not found",
  );
  await check(
    "main delivery cannot be read through a task",
    ["status", "Ada", "delivery-2", "--task", "task-1", "--json"],
    1,
    (run) => parsed(run).error === "not found",
  );
  const firstTask = tasks.get("task-1");
  assert.ok(firstTask);
  tasks.set("task-2", { ...firstTask, id: "task-2", threadId: "thread-task-2" });
  await check(
    "a sibling task's delivery is refused",
    ["status", "Ada", taskDelivery.id, "--task", "task-2", "--json"],
    1,
    (run) => parsed(run).error === "not found",
  );
  await check(
    "main chat still has its own result",
    ["status", "Ada", "delivery-2", "--json"],
    0,
    (run) => parsed(run).answer === "Answer of turn-delivery-2",
  );
  tasks.set("task-1", { ...firstTask, state: "closed", closedAt: new Date().toISOString() });
  await check(
    "closed task refuses a send",
    ["run", "Ada", "hello", "--task", "task-1", "--json"],
    1,
    () => deliveries.size === count,
  );
  await check(
    "conflicting task flags send nothing",
    ["run", "Ada", "hello", "--new-task", "--task", "task-1", "--json"],
    2,
    () => deliveries.size === count,
  );
  await Effect.runPromise(server.stop());
  await check("a stopped app exits 5", ["list"], 5, (run) => run.stderr.includes("OpenBot is not running"));
  await check("a wrong command line exits 2 also when the app is stopped", ["agent", "foo", "Ada"], 2, (run) =>
    run.stderr.includes("Usage:"),
  );
  checks.push({
    name: "the token is never in the output",
    passed: outputs.every((text) => !text.includes(token) && !text.includes(newToken)),
    detail: null,
  });
} finally {
  await Effect.runPromise(server.stop());
  rmSync(root, { recursive: true, force: true });
}

const failed = checks.filter((item) => !item.passed);
writeFileSync(join(OUT, "report.json"), `${JSON.stringify({ passed: failed.length === 0, checks }, null, 2)}\n`);
for (const item of checks) process.stdout.write(`${item.passed ? "ok" : "FAILED"} ${item.name}\n`);
assert.equal(failed.length, 0, `${failed.length} checks failed; see ${join(OUT, "report.json")}`);

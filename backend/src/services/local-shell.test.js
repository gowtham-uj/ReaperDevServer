import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";

const root = await fs.mkdtemp(path.join(os.tmpdir(), "reaper-shell-test-"));
const caddyFile = path.join(root, "caddy", "ports.caddy");
const stateRoot = path.join(root, "state");
process.env.VPS_PROJECTS = root;
process.env.STATE_DIR = stateRoot;
process.env.CADDY_DYNAMIC_FILE = caddyFile;
process.env.APEX_DOMAIN = "example.test";
process.env.REAPER_HOST = "example.test";

const shell = await import("./local-shell.js");
const protocol = await import("./terminal-protocol.js");

function makeFakeRuntime() {
  const calls = [];
  const ptys = [];
  const installed = [];
  const legacyFiles = new Map();
  let reloads = 0;
  let reloadFailures = 0;
  let ensurePodCalls = 0;
  const controlPlans = new Map();
  const captureDelays = new Map();
  const captureCompletions = new Map();
  const sessions = new Map();
  let sendKeysGate = null;
  let activeSendKeys = 0;
  let maxActiveSendKeys = 0;
  const sendKeysStarted = [];
  let controlCommandId = 10;
  return {
    calls,
    ptys,
    installed,
    get reloads() { return reloads; },
    get ensurePodCalls() { return ensurePodCalls; },
    failNextReload() { reloadFailures += 1; },
    dropSession(project, name) { sessions.get(project)?.delete(name); },
    hasRunningSession(project, name) { return Boolean(sessions.get(project)?.has(name)); },
    failNextCapture(project, result = { stderr: "permission denied" }) {
      controlPlans.set(project, [{
        captureChunks: [`%begin 2 2 0\n${result.stderr}\n%error 2 2 0\n`]
      }]);
    },
    queueControlPlan(project, plan) {
      const plans = controlPlans.get(project) || [];
      plans.push(plan);
      controlPlans.set(project, plans);
    },
    delayNextCapture(project) {
      let release;
      const promise = new Promise((resolve) => { release = resolve; });
      captureDelays.set(project, promise);
      return release;
    },
    captureCompletionCount(project) { return captureCompletions.get(project) || 0; },
    delayNextSendKeys() {
      let release;
      const promise = new Promise((resolve) => { release = resolve; });
      sendKeysGate = promise;
      return release;
    },
    get maxActiveSendKeys() { return maxActiveSendKeys; },
    sendKeysStarted,
    setLegacyFile(project, file, content) { legacyFiles.set(`${project}:${file}`, content); },
    addRunningSession(project, name) {
      if (!sessions.has(project)) sessions.set(project, new Set());
      sessions.get(project).add(name);
    },
    async podAvailable() { return { available: true }; },
    async ensurePod(project) {
      ensurePodCalls += 1;
      if (!sessions.has(project)) sessions.set(project, new Set());
      return { name: `pod-${project}`, ip: "172.30.1.9" };
    },
    async podExec(project, argv, options = {}) {
      calls.push({ project, argv: [...argv], options: { ...options } });
      if (argv[0] === "sh" && argv[3] === "reaper-install") {
        installed.push({ project, target: argv[4], content: options.input });
      }
      if (argv[0] === "timeout" && argv[2] === "cat") {
        const key = `${project}:${argv.at(-1)}`;
        return legacyFiles.has(key)
          ? { code: 0, stdout: legacyFiles.get(key), stderr: "" }
          : { code: 1, stdout: "", stderr: "No such file or directory" };
      }
      const names = sessions.get(project) || new Set();
      sessions.set(project, names);
      if (argv[0] === "sh" && argv[3] === "reaper-prepare-session") {
        names.add(argv[6]);
        return { code: 0, stdout: "", stderr: "" };
      }
      const command = argv.find((value) => ["has-session", "new-session", "kill-session", "list-sessions", "capture-pane", "send-keys"].includes(value));
      const target = String(argv[argv.indexOf("-t") + 1] || "").replace(/^=/, "").replace(/:$/, "");
      if (command === "has-session") return { code: names.has(target) ? 0 : 1, stdout: "", stderr: "" };
      if (command === "new-session") { names.add(argv[argv.indexOf("-s") + 1]); return { code: 0, stdout: "", stderr: "" }; }
      if (command === "kill-session") {
        if (!names.has(target)) return { code: 1, stdout: "", stderr: `can't find session: ${target}` };
        names.delete(target);
        return { code: 0, stdout: "", stderr: "" };
      }
      if (command === "list-sessions") return { code: 0, stdout: [...names].join("\n"), stderr: "" };
      if (command === "send-keys") {
        const input = argv.at(-1);
        sendKeysStarted.push(input);
        activeSendKeys += 1;
        maxActiveSendKeys = Math.max(maxActiveSendKeys, activeSendKeys);
        const delayed = sendKeysGate;
        sendKeysGate = null;
        if (delayed) await delayed;
        activeSendKeys -= 1;
        return { code: 0, stdout: "", stderr: "" };
      }
      if (command === "capture-pane") {
        return names.has(target)
          ? { code: 0, stdout: "old\u001b[6n\nline\n", stderr: "" }
          : { code: 1, stdout: "", stderr: `can't find session: ${target}` };
      }
      return { code: 0, stdout: "", stderr: "" };
    },
    podExecPty(project, argv, options = {}) {
      const names = sessions.get(project) || new Set();
      sessions.set(project, names);
      const isControl = argv[0] === "env" && argv[5] === "reaper-capture-session";
      const name = isControl ? argv[8] : "";
      const plans = controlPlans.get(project) || [];
      const plan = plans.shift() || {};
      controlPlans.set(project, plans);
      const emitChunks = async (pty, chunks) => {
        for (const chunk of chunks) {
          if (pty.killed) return;
          pty.dataCallback?.(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
          await tick();
        }
      };
      const defaultCapture = [
        "%output %1 BEFORE\n",
        "%begin 2 2 0\n",
        "old\u001b[6n\n",
        "%output %1 DURING\n",
        "line\n",
        "%end 2 2 0\n",
        "%output %1 LIVE\n"
      ];
      const pty = {
        project,
        argv: [...argv],
        options: { ...options },
        killed: false,
        paused: false,
        writes: [],
        resizes: [],
        onData(callback) {
          this.dataCallback = callback;
          queueMicrotask(async () => {
            if (!isControl) return callback("LIVE");
            if (!names.has(name)) {
              callback("REAPER_SESSION_MISSING\n");
              this.exitCallback?.({ exitCode: 42 });
              return;
            }
            await emitChunks(this, plan.initialChunks || ["\u001bP1000p%begin 1 1 0\n%end 1 1 0\n"]);
          });
        },
        onExit(callback) { this.exitCallback = callback; },
        write(data) {
          this.writes.push(Buffer.from(data));
          const command = String(data);
          if (isControl && command.startsWith("display-message ")) {
            queueMicrotask(() => emitChunks(
              this,
              plan.modeChunks || ["%begin 2 2 0\n1,1,1,1,0,0,0,0,0,1\n%end 2 2 0\n"]
            ));
          }
          if (isControl && command.startsWith("refresh-client ")) {
            queueMicrotask(() => emitChunks(
              this,
              plan.resizeChunks || ["%begin 2 2 0\n%end 2 2 0\n"]
            ));
          }
          if (isControl && command.startsWith("send-keys ")) {
            const id = ++controlCommandId;
            const bytes = command.trim().split(" -H ")[1]?.split(/\s+/).map((value) => Number.parseInt(value, 16)) || [];
            const input = Buffer.from(bytes).toString("utf8");
            queueMicrotask(async () => {
              sendKeysStarted.push(input);
              activeSendKeys += 1;
              maxActiveSendKeys = Math.max(maxActiveSendKeys, activeSendKeys);
              this.dataCallback?.(`%begin ${id} ${id} 0\n`);
              const delayed = sendKeysGate;
              sendKeysGate = null;
              if (delayed) await delayed;
              activeSendKeys -= 1;
              this.dataCallback?.(`%end ${id} ${id} 0\n`);
            });
          }
          if (isControl && command.startsWith("capture-pane ")) {
            queueMicrotask(async () => {
              const delayed = captureDelays.get(project);
              if (delayed) {
                captureDelays.delete(project);
                await delayed;
              }
              if (plan.respond) await plan.respond(this);
              else await emitChunks(this, plan.captureChunks || defaultCapture);
              captureCompletions.set(project, (captureCompletions.get(project) || 0) + 1);
              if (plan.exitCode !== undefined) this.exitCallback?.({ exitCode: plan.exitCode });
            });
          }
          return true;
        },
        resize(cols, rows) { this.resizes.push([cols, rows]); },
        pause() { this.paused = true; },
        resume() { this.paused = false; },
        kill() {
          if (this.killed) return;
          this.killed = true;
          this.exitCallback?.({ exitCode: 0 });
        }
      };
      ptys.push(pty);
      return pty;
    },
    async podInspect(project) { return { exists: true, running: true, isolated: true, ip: "172.30.1.9", generation: `pod-${project}-id` }; },
    async reloadCaddy() {
      reloads += 1;
      if (reloadFailures > 0) {
        reloadFailures -= 1;
        throw new Error("simulated Caddy reload failure");
      }
    },
    async destroyPod() {},
    async listPods() { return []; },
    podName(project) { return `pod-${project}`; }
  };
}

const fake = makeFakeRuntime();
shell.__setPodRuntimeForTests(fake);
await shell.selectBackend();

async function createProject(name) {
  await fs.mkdir(path.join(root, name), { recursive: true });
}

function expectedPublishedBlock(subdomain, containerPort, requireReaperAuth = true) {
  const forwardAuth = requireReaperAuth
    ? "\n\tforward_auth 127.0.0.1:4000 {\n\t\turi /api/auth/me\n\t}"
    : "";
  return `https://${subdomain}.example.test {\n\theader {\n\t\t-Server\n\t\tX-Content-Type-Options "nosniff"\n\t\tX-Frame-Options "SAMEORIGIN"\n\t\tReferrer-Policy "same-origin"\n\t\tX-Robots-Tag "noindex, nofollow, noarchive"\n\t\tStrict-Transport-Security "max-age=31536000; includeSubDomains"\n\t}${forwardAuth}\n\treverse_proxy 172.30.1.9:${containerPort} {\n\t\theader_up Cookie "(^|;[[:space:]]*)reaper_access=[^;]*" ""\n\t\theader_up Cookie "(^|;[[:space:]]*)reaper_csrf=[^;]*" ""\n\t\theader_down Set-Cookie "^reaper_(access|csrf)=.*$" ""\n\t}\n}`;
}

function expectedIpPublishedBlock(containerPort) {
  return `https://167.86.121.124:${containerPort} {\n\ttls {\n\t\tissuer acme {\n\t\t\tdir https://acme-v02.api.letsencrypt.org/directory\n\t\t\tprofile shortlived\n\t\t}\n\t}\n\theader {\n\t\t-Server\n\t\tX-Content-Type-Options "nosniff"\n\t\tX-Frame-Options "SAMEORIGIN"\n\t\tReferrer-Policy "same-origin"\n\t\tX-Robots-Tag "noindex, nofollow, noarchive"\n\t\tStrict-Transport-Security "max-age=31536000; includeSubDomains"\n\t}\n\tforward_auth 127.0.0.1:4000 {\n\t\turi /api/auth/me\n\t}\n\treverse_proxy 172.30.1.9:${containerPort} {\n\t\theader_up Cookie "(^|;[[:space:]]*)reaper_access=[^;]*" ""\n\t\theader_up Cookie "(^|;[[:space:]]*)reaper_csrf=[^;]*" ""\n\t\theader_down Set-Cookie "^reaper_(access|csrf)=.*$" ""\n\t}\n}`;
}

function tick(ms = 0) { return new Promise((resolve) => setTimeout(resolve, ms)); }
async function waitFor(predicate, timeoutMs = 1000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("timed out waiting for condition");
    await tick(5);
  }
}

class FakeWebSocket extends EventEmitter {
  constructor() { super(); this.readyState = 1; this.bufferedAmount = 0; this.frames = []; this.terminated = false; }
  send(data) { this.frames.push(protocol.decodeFrame(data)); }
  close(code, reason) { this.readyState = 3; this.closeCode = code; this.closeReason = reason; this.emit("close"); }
  terminate() { this.terminated = true; if (this.readyState !== 3) this.close(); }
}
async function attachFakeTerminal(wss, project, requestId, { cols = 100, rows = 30, waitForReady = true } = {}) {
  const ws = new FakeWebSocket();
  wss.emit("connection", ws, { socket: { setNoDelay() {} }, reaperAuth: { sub: `test-${requestId}` } });
  ws.emit("message", protocol.encodeFrame({
    type: protocol.TYPES.HELLO,
    streamId: 0,
    sequence: 1,
    payload: protocol.encodeJson({ csrfToken: "csrf", clientVersion: "1", capabilities: ["binary", "multiplex", "history"] })
  }), true);
  await waitFor(() => ws.frames.some((frame) => frame.type === protocol.TYPES.HELLO_ACK));
  ws.emit("message", protocol.encodeFrame({
    type: protocol.TYPES.OPEN,
    streamId: 0,
    sequence: 2,
    payload: protocol.encodeJson({ requestId, project, sessionName: "main", cols, rows })
  }), true);
  if (waitForReady) await waitFor(() => ws.frames.some((frame) => frame.type === protocol.TYPES.READY));
  return ws;
}

async function connectFakeTerminal(wss, userId = "test-user") {
  const ws = new FakeWebSocket();
  wss.emit("connection", ws, { socket: { setNoDelay() {} }, reaperAuth: { sub: userId } });
  if (ws.readyState !== 1) return ws;
  ws.emit("message", protocol.encodeFrame({
    type: protocol.TYPES.HELLO,
    streamId: 0,
    sequence: 1,
    payload: protocol.encodeJson({ csrfToken: "csrf", clientVersion: "1", capabilities: ["binary", "multiplex", "history"] })
  }), true);
  await waitFor(() => ws.frames.some((frame) => frame.type === protocol.TYPES.HELLO_ACK));
  return ws;
}


test("stripTerminalQueries removes query families and preserves terminal output", () => {
  const keep = "plain π \u001b[31mred\u001b[0m \u001b[2A \u001b]0;title\u0007";
  const queries = [
    "\u001b[5n", "\u001b[6n", "\u001b[?6n",
    "\u001b[c", "\u001b[0c", "\u001b[>c", "\u001b[>0c", "\u001b[=c",
    "\u001bP$qm\u001b\\", "\u001bP+q544e\u001b\\",
    "\u001b]4;12;?\u0007", "\u001b]4;2;?\u001b\\",
    "\u001b]10;?\u0007", "\u001b]11;?\u001b\\", "\u001b]12;?\u0007",
    "\u001b[?u", "\u001b[?4$p"
  ].join("");
  assert.equal(shell.stripTerminalQueries(`${keep}${queries}tail`), `${keep}tail`);
});

test("manifest supports N persistent sessions across manager operations", async () => {
  await createProject("alpha");
  const ensurePodCallsBefore = fake.ensurePodCalls;
  await shell.openProjectShell({ path: "alpha", sessionName: "main" });
  await shell.openProjectShell({ path: "alpha", sessionName: "bot", title: "Discord bot" });
  await shell.openProjectShell({ path: "alpha", sessionName: "worker", title: "Worker" });
  assert.deepEqual((await shell.listSessions({ path: "alpha" })).map((item) => item.name), ["main", "bot", "worker"]);
  const workerPreparation = fake.calls.find((call) => call.argv[3] === "reaper-prepare-session" && call.argv[6] === "worker");
  assert.ok(workerPreparation);
  assert.match(workerPreparation.argv[2], /pipe-pane/);
  assert.equal(fake.hasRunningSession("alpha", "worker"), true);
  assert.equal(fake.ensurePodCalls - ensurePodCallsBefore, 4);
  const renamed = await shell.renameSession("alpha", "bot", "Production bot");
  assert.equal(renamed.session.title, "Production bot");
  const disk = JSON.parse(await fs.readFile(path.join(stateRoot, "projects", "alpha", "sessions.json"), "utf8"));
  assert.deepEqual(disk.map((item) => item.name), ["main", "bot", "worker"]);
  assert.equal(disk[1].title, "Production bot");
  await fs.mkdir(path.join(root, ".hidden-control"), { recursive: true });
  await shell.initLocalShells();
  assert.deepEqual((await shell.listSessions({ path: "alpha" })).map((item) => item.name), ["main", "bot", "worker"]);
  assert.equal(fake.calls.some((call) => call.project === ".hidden-control"), false);
  assert.equal(fake.calls.filter((call) => call.argv.includes("kill-session")).length, 0);
  assert.ok(fake.calls.some((call) => call.project === "alpha" && call.argv.some((arg) => arg.includes("list-clients"))));
  assert.ok(fake.calls.some((call) => call.project === "alpha" && call.argv.includes("REAPER_VIEWER_ID=") && call.argv.at(-1) === "prefix"));
  await shell.destroySession("worker", { project: "alpha" });
  assert.equal(fake.calls.filter((call) => call.argv.includes("kill-session")).length, 1);
  assert.ok(fake.calls.some((call) => call.argv.includes("REAPER_SESSION_ID=worker")));
  assert.ok(fake.calls.some((call) => call.argv[3] === "reaper-prepare-session" && call.argv[9].endsWith(" 'worker'")));
  assert.deepEqual((await shell.listSessions({ path: "alpha" })).map((item) => item.name), ["main", "bot"]);
});

test("legacy pod metadata migrates into trusted backend state without losing named sessions", async () => {
  await createProject("legacy");
  fake.setLegacyFile("legacy", "/work/.reaper/sessions.json", JSON.stringify([
    { name: "main", title: "Main shell", createdAt: "2026-01-01T00:00:00.000Z" },
    { name: "bot", title: "Discord bot", createdAt: "2026-01-02T00:00:00.000Z" }
  ]));
  fake.setLegacyFile("legacy", "/work/.reaper/env.json", JSON.stringify({ LEGACY_TOKEN: "preserved" }));
  fake.setLegacyFile("legacy", "/work/.reaper/bashrc", "alias legacy='yes'\n");
  fake.setLegacyFile("legacy", "/work/.reaper/ports.json", JSON.stringify([{ containerPort: 8080, subdomain: "legacy-app" }]));
  fake.addRunningSession("legacy", "worker");

  assert.deepEqual(
    (await shell.listSessions({ path: "legacy" })).map((session) => session.name),
    ["main", "bot", "worker"]
  );
  assert.deepEqual({ ...(await shell.getProjectEnv("legacy")) }, { LEGACY_TOKEN: "preserved" });
  assert.equal(await shell.getProjectBashrc("legacy"), "alias legacy='yes'\n");
  assert.deepEqual(await shell.getProjectPorts("legacy"), {
    ports: [{ containerPort: 8080, subdomain: "legacy-app" }],
    requireReaperAuth: true
  });
  assert.deepEqual(
    JSON.parse(await fs.readFile(path.join(stateRoot, "projects", "legacy", "sessions.json"), "utf8")).map((session) => session.name),
    ["main", "bot", "worker"]
  );
  await shell.updateProjectPorts("legacy", []);
});

test("legacy session migration rejects oversized manifests before preparing sessions", async () => {
  await createProject("legacy-overflow");
  fake.setLegacyFile(
    "legacy-overflow",
    "/work/.reaper/sessions.json",
    JSON.stringify(Array.from({ length: 33 }, (_, index) => ({ name: `s${index}` })))
  );
  const preparationsBefore = fake.calls.filter((call) => call.argv[3] === "reaper-prepare-session").length;
  await assert.rejects(shell.listSessions({ path: "legacy-overflow" }), /cannot exceed 32 sessions/);
  assert.equal(fake.calls.filter((call) => call.argv[3] === "reaper-prepare-session").length, preparationsBefore);
  await assert.rejects(fs.access(path.join(stateRoot, "projects", "legacy-overflow", "control.json")));
  await fs.rm(path.join(root, "legacy-overflow"), { recursive: true, force: true });
});

test("RTP open orders history before ready/output and disconnect only detaches", async () => {
  await createProject("transport");
  await shell.openProjectShell({ path: "transport", sessionName: "main" });
  const wss = new EventEmitter();
  shell.attachTerminalWebSocket(wss, { verifyCsrf: (_req, token) => token === "csrf" });
  const ws = new FakeWebSocket();
  const request = { socket: { setNoDelay() {} } };
  wss.emit("connection", ws, request);
  ws.emit("message", protocol.encodeFrame({ type: protocol.TYPES.HELLO, streamId: 0, sequence: 1, payload: protocol.encodeJson({ csrfToken: "csrf", clientVersion: "1", capabilities: ["binary", "multiplex", "history"] }) }), true);
  ws.emit("message", protocol.encodeFrame({ type: protocol.TYPES.OPEN, streamId: 0, sequence: 2, payload: protocol.encodeJson({ requestId: "r1", project: "transport", sessionName: "main", cols: 100, rows: 30 }) }), true);
  await waitFor(() => ws.frames.some((frame) => frame.type === protocol.TYPES.OUTPUT));
  const types = ws.frames.map((frame) => frame.type);
  const opened = types.indexOf(protocol.TYPES.OPENED);
  const history = types.indexOf(protocol.TYPES.HISTORY);
  const ready = types.indexOf(protocol.TYPES.READY);
  const output = types.indexOf(protocol.TYPES.OUTPUT);
  assert.ok(opened >= 0 && opened < history && history < ready && ready < output, types.join(","));
  const historyFrame = ws.frames[history];
  assert.equal(historyFrame.flags, protocol.FLAGS.FINAL);
  const renderedHistory = Buffer.from(historyFrame.payload).toString();
  assert.match(renderedHistory, /^\u001b\[\?1049h/);
  assert.ok(renderedHistory.endsWith("old\r\nline"));
  const streamId = ws.frames[opened].streamId;
  const pty = fake.ptys.at(-1);
  assert.equal(pty.argv[0], "env");
  assert.match(pty.argv[1], /^REAPER_VIEWER_ID=[0-9a-f-]{36}$/);
  assert.equal(pty.argv[2], "sh");
  assert.equal(pty.argv[5], "reaper-capture-session");
  const emoji = Buffer.from("😀", "utf8");
  const inputsBefore = fake.sendKeysStarted.length;
  ws.emit("message", protocol.encodeFrame({ type: protocol.TYPES.INPUT, streamId, sequence: 1, payload: emoji.subarray(0, 2) }), true);
  await tick();
  assert.equal(fake.sendKeysStarted.length, inputsBefore);
  ws.emit("message", protocol.encodeFrame({ type: protocol.TYPES.INPUT, streamId, sequence: 2, payload: emoji.subarray(2) }), true);
  await waitFor(() => fake.sendKeysStarted.length === inputsBefore + 1);
  assert.equal(fake.sendKeysStarted.at(-1), "😀");
  assert.ok(pty.writes.some((write) => write.toString().endsWith("-H f0 9f 98 80\n")));
  ws.emit("message", protocol.encodeFrame({ type: protocol.TYPES.RESIZE, streamId, sequence: 0, payload: protocol.encodeResize(100, 30) }), true);
  ws.emit("message", protocol.encodeFrame({ type: protocol.TYPES.RESIZE, streamId, sequence: 0, payload: protocol.encodeResize(100, 30) }), true);
  ws.emit("message", protocol.encodeFrame({ type: protocol.TYPES.RESIZE, streamId, sequence: 0, payload: protocol.encodeResize(120, 40) }), true);
  ws.emit("message", protocol.encodeFrame({ type: protocol.TYPES.RESIZE, streamId, sequence: 0, payload: protocol.encodeResize(120, 40) }), true);
  await waitFor(() => pty.resizes.length === 1 && pty.writes.some((write) => write.toString() === "refresh-client -C 120,40\n"));
  assert.deepEqual(pty.resizes, [[120, 40]]);
  assert.deepEqual(
    pty.writes.filter((write) => write.toString().startsWith("refresh-client ")).map((write) => write.toString()),
    ["refresh-client -C 100,30\n", "refresh-client -C 120,40\n"]
  );
  const liveChunk = `%output %1 ${"a".repeat(128 * 1024)}\n`;
  for (let index = 0; index < 24; index += 1) {
    pty.dataCallback(liveChunk);
    await tick(3);
  }
  await waitFor(() => pty.paused);
  const lastOutput = ws.frames.filter((frame) => frame.type === protocol.TYPES.OUTPUT).at(-1);
  ws.emit("message", protocol.encodeFrame({ type: protocol.TYPES.ACK, streamId, sequence: lastOutput.sequence, payload: Buffer.alloc(0) }), true);
  await waitFor(() => !pty.paused);
  ws.close(1000, "reconnect");
  await waitFor(() => pty.killed && fake.calls.some((call) => call.argv.includes(pty.argv[1]) && call.argv.at(-1) === "exact"));
  assert.equal(fake.calls.filter((call) => call.argv.includes("kill-session") && call.project === "transport").length, 0);
  await shell.destroySession("main", { project: "transport" });
  assert.equal(fake.calls.filter((call) => call.argv.includes("kill-session") && call.project === "transport").length, 1);
  const targets = fake.calls
    .filter((call) => call.project === "transport" && call.argv.includes("-t"))
    .map((call) => call.argv[call.argv.indexOf("-t") + 1].replace(/:$/, ""));
  assert.ok(targets.length > 0);
  assert.deepEqual([...new Set(targets)], ["=main"]);
  assert.equal(pty.argv[5], "reaper-capture-session");
});

test("concurrent viewers share the smallest canonical pane viewport", async () => {
  await createProject("shared-viewport");
  await shell.openProjectShell({ path: "shared-viewport", sessionName: "main" });
  const wss = new EventEmitter();
  shell.attachTerminalWebSocket(wss, { verifyCsrf: (_req, token) => token === "csrf" });

  const first = await attachFakeTerminal(wss, "shared-viewport", "viewport-first", { cols: 100, rows: 30 });
  const firstStreamId = first.frames.find((frame) => frame.type === protocol.TYPES.OPENED).streamId;
  const firstPty = fake.ptys.at(-1);
  const second = await attachFakeTerminal(wss, "shared-viewport", "viewport-second", { cols: 140, rows: 40 });
  const secondOpened = second.frames.find((frame) => frame.type === protocol.TYPES.OPENED);
  const secondStreamId = secondOpened.streamId;
  const secondPty = fake.ptys.at(-1);

  assert.deepEqual(secondPty.options, { cols: 100, rows: 30 });
  assert.deepEqual(protocol.decodeJson(secondOpened.payload), {
    requestId: "viewport-second",
    project: "shared-viewport",
    sessionName: "main",
    title: "main",
    degraded: false,
    cols: 100,
    rows: 30
  });

  first.emit("message", protocol.encodeFrame({
    type: protocol.TYPES.RESIZE,
    streamId: firstStreamId,
    sequence: 0,
    payload: protocol.encodeResize(120, 35)
  }), true);
  await waitFor(() =>
    firstPty.resizes.some(([cols, rows]) => cols === 120 && rows === 35) &&
    secondPty.resizes.some(([cols, rows]) => cols === 120 && rows === 35)
  );
  const latestViewport = (ws) => protocol.decodeJson(
    ws.frames.filter((frame) => frame.type === protocol.TYPES.STATUS).at(-1).payload
  );
  assert.deepEqual(latestViewport(first), { state: "viewport", cols: 120, rows: 35 });
  assert.deepEqual(latestViewport(second), { state: "viewport", cols: 120, rows: 35 });

  first.close(1000, "smaller viewer left");
  await waitFor(() => {
    const viewport = latestViewport(second);
    return viewport.cols === 140 && viewport.rows === 40;
  });
  assert.deepEqual(latestViewport(second), { state: "viewport", cols: 140, rows: 40 });
  second.close(1000, "done");
});

test("pod input bursts are coalesced in order, serialized, and update live activity metadata", async () => {
  await createProject("input-order");
  await shell.openProjectShell({ path: "input-order", sessionName: "main" });
  const wss = new EventEmitter();
  shell.attachTerminalWebSocket(wss, { verifyCsrf: (_req, token) => token === "csrf" });
  const ws = await attachFakeTerminal(wss, "input-order", "input-order-1", { cols: 100, rows: 30 });
  const streamId = ws.frames.find((frame) => frame.type === protocol.TYPES.OPENED).streamId;
  const startedAt = fake.sendKeysStarted.length;
  const release = fake.delayNextSendKeys();

  ws.emit("message", protocol.encodeFrame({ type: protocol.TYPES.INPUT, streamId, sequence: 1, payload: Buffer.from("first") }), true);
  await waitFor(() => fake.sendKeysStarted.length === startedAt + 1);
  ws.emit("message", protocol.encodeFrame({ type: protocol.TYPES.INPUT, streamId, sequence: 2, payload: Buffer.from("second") }), true);
  ws.emit("message", protocol.encodeFrame({ type: protocol.TYPES.INPUT, streamId, sequence: 3, payload: Buffer.from("third") }), true);
  await tick(20);
  assert.deepEqual(fake.sendKeysStarted.slice(startedAt), ["first"]);
  release();
  await waitFor(() => fake.sendKeysStarted.length === startedAt + 2);
  await waitFor(() => ws.frames.filter((frame) => frame.type === protocol.TYPES.ACK).some((frame) => frame.sequence === 3));
  assert.deepEqual(fake.sendKeysStarted.slice(startedAt), ["first", "secondthird"]);
  assert.equal(fake.maxActiveSendKeys, 1);
  assert.equal(fake.calls.some((call) => call.project === "input-order" && call.argv.includes("send-keys")), false);

  const active = (await shell.listSessions({ path: "input-order" })).find((session) => session.name === "main");
  assert.equal(active.attachedClients, 1);
  assert.match(active.lastInteractionAt, /^\d{4}-\d{2}-\d{2}T/);
  ws.emit("message", protocol.encodeFrame({ type: protocol.TYPES.RESIZE, streamId, payload: protocol.encodeResize(65535, 65535) }), true);
  await waitFor(() => fake.ptys.at(-1).resizes.some(([cols, rows]) => cols === 500 && rows === 300));
  ws.close(1000, "done");
  assert.equal((await shell.listSessions({ path: "input-order" }))[0].attachedClients, 0);
});

test("future and regressive cumulative ACKs close the RTP connection", async () => {
  await createProject("ack-bounds");
  await shell.openProjectShell({ path: "ack-bounds", sessionName: "main" });
  const wss = new EventEmitter();
  shell.attachTerminalWebSocket(wss, { verifyCsrf: (_req, token) => token === "csrf" });

  const future = await attachFakeTerminal(wss, "ack-bounds", "ack-future");
  const futureStream = future.frames.find((frame) => frame.type === protocol.TYPES.OPENED).streamId;
  future.emit("message", protocol.encodeFrame({ type: protocol.TYPES.ACK, streamId: futureStream, sequence: 0xffffffff }), true);
  await waitFor(() => future.readyState === 3);
  assert.equal(future.closeReason, "ACK_SEQUENCE");

  const regressive = await attachFakeTerminal(wss, "ack-bounds", "ack-regressive");
  const regressiveStream = regressive.frames.find((frame) => frame.type === protocol.TYPES.OPENED).streamId;
  const readySequence = regressive.frames.find((frame) => frame.type === protocol.TYPES.READY).sequence;
  regressive.emit("message", protocol.encodeFrame({ type: protocol.TYPES.ACK, streamId: regressiveStream, sequence: readySequence }), true);
  regressive.emit("message", protocol.encodeFrame({ type: protocol.TYPES.ACK, streamId: regressiveStream, sequence: readySequence - 1 }), true);
  await waitFor(() => regressive.readyState === 3);
  assert.equal(regressive.closeReason, "ACK_SEQUENCE");
});

test("OPEN fan-out, connection count, PING rate, PONG correlation, and buffered output are bounded", async () => {
  await createProject("transport-bounds");
  await shell.openProjectShell({ path: "transport-bounds", sessionName: "main" });
  const wss = new EventEmitter();
  shell.attachTerminalWebSocket(wss, { verifyCsrf: (_req, token) => token === "csrf" });

  const opens = await connectFakeTerminal(wss, "open-flood");
  for (let index = 0; index < 3; index += 1) {
    opens.emit("message", protocol.encodeFrame({
      type: protocol.TYPES.OPEN,
      streamId: 0,
      sequence: index + 2,
      payload: protocol.encodeJson({ requestId: `flood-${index}`, project: "transport-bounds", sessionName: "main", cols: 80, rows: 24 })
    }), true);
  }
  await waitFor(() => opens.readyState === 3);
  assert.equal(opens.closeReason, "OPEN_LIMIT");

  const buffered = await connectFakeTerminal(wss, "buffered");
  buffered.bufferedAmount = protocol.MAX_BUFFERED_AMOUNT;
  buffered.emit("message", protocol.encodeFrame({ type: protocol.TYPES.PING, streamId: 0, sequence: 7, payload: protocol.encodePing(1) }), true);
  await waitFor(() => buffered.readyState === 3);
  assert.equal(buffered.closeCode, 1009);

  const pingFlood = await connectFakeTerminal(wss, "ping-flood");
  pingFlood.emit("message", protocol.encodeFrame({ type: protocol.TYPES.PING, streamId: 0, sequence: 8, payload: protocol.encodePing(2) }), true);
  pingFlood.emit("message", protocol.encodeFrame({ type: protocol.TYPES.PING, streamId: 0, sequence: 9, payload: protocol.encodePing(3) }), true);
  await waitFor(() => pingFlood.readyState === 3);
  assert.equal(pingFlood.closeReason, "PING_RATE");

  const forgedPong = await connectFakeTerminal(wss, "forged-pong");
  forgedPong.emit("message", protocol.encodeFrame({ type: protocol.TYPES.PONG, streamId: 0, sequence: 99, payload: protocol.encodePing(4) }), true);
  await waitFor(() => forgedPong.readyState === 3);
  assert.equal(forgedPong.closeReason, "PONG_SEQUENCE");

  const userConnections = [];
  for (let index = 0; index < 9; index += 1) {
    userConnections.push(await connectFakeTerminal(wss, "connection-flood"));
  }
  assert.equal(userConnections.filter((socket) => socket.readyState === 1).length, 8);
  assert.equal(userConnections.at(-1).closeCode, 1008);
  assert.equal(userConnections.at(-1).terminated, true);
  for (const socket of userConnections) if (socket.readyState === 1) socket.close(1000, "done");
});
test("warm attach and reconnect skip session provisioning", async () => {
  await createProject("warm-attach");
  await shell.openProjectShell({ path: "warm-attach", sessionName: "main" });
  const wss = new EventEmitter();
  shell.attachTerminalWebSocket(wss, { verifyCsrf: (_req, token) => token === "csrf" });
  const preparationCount = () => fake.calls.filter((call) => call.project === "warm-attach" && call.argv[3] === "reaper-prepare-session").length;
  const preparedBefore = preparationCount();
  const installsBefore = fake.installed.filter((item) => item.project === "warm-attach").length;
  const ensurePodCallsBefore = fake.ensurePodCalls;
  const ptysBefore = fake.ptys.length;
  const assertOrderedOpen = (ws) => {
    const types = ws.frames.map((frame) => frame.type);
    const opened = types.indexOf(protocol.TYPES.OPENED);
    const history = types.indexOf(protocol.TYPES.HISTORY);
    const ready = types.indexOf(protocol.TYPES.READY);
    assert.ok(opened >= 0 && opened < history && history < ready, types.join(","));
    assert.equal(protocol.decodeJson(ws.frames[opened].payload).sessionName, "main");
  };

  const first = await attachFakeTerminal(wss, "warm-attach", "warm-1");
  assertOrderedOpen(first);
  const firstPty = fake.ptys.at(-1);
  first.close(1000, "reconnect");
  await waitFor(() => firstPty.killed);

  const second = await attachFakeTerminal(wss, "warm-attach", "warm-2");
  assertOrderedOpen(second);
  const secondPty = fake.ptys.at(-1);
  assert.equal(preparationCount(), preparedBefore);
  assert.equal(fake.installed.filter((item) => item.project === "warm-attach").length, installsBefore);
  assert.equal(fake.ensurePodCalls - ensurePodCallsBefore, 0);
  assert.equal(fake.ptys.length - ptysBefore, 2);
  assert.equal(fake.calls.filter((call) => call.project === "warm-attach" && call.argv.includes("kill-session")).length, 0);
  second.close(1000, "done");
  await waitFor(() => secondPty.killed);
});

test("fast attach repairs an externally missing tmux session once", async () => {
  await createProject("attach-recovery");
  await shell.openProjectShell({ path: "attach-recovery", sessionName: "main" });
  fake.dropSession("attach-recovery", "main");
  const wss = new EventEmitter();
  shell.attachTerminalWebSocket(wss, { verifyCsrf: (_req, token) => token === "csrf" });
  const preparationsBefore = fake.calls.filter((call) => call.project === "attach-recovery" && call.argv[3] === "reaper-prepare-session").length;

  const ws = await attachFakeTerminal(wss, "attach-recovery", "repair-1");
  assert.equal(fake.hasRunningSession("attach-recovery", "main"), true);
  assert.equal(fake.calls.filter((call) => call.project === "attach-recovery" && call.argv[3] === "reaper-prepare-session").length, preparationsBefore + 1);
  assert.equal(fake.ptys.filter((item) => item.project === "attach-recovery").length, 2);
  const pty = fake.ptys.at(-1);
  ws.close(1000, "done");
  await waitFor(() => pty.killed);
});
test("unknown capture failure does not recreate a session or expose a stream", async () => {
  await createProject("capture-error");
  await shell.openProjectShell({ path: "capture-error", sessionName: "main" });
  fake.failNextCapture("capture-error");
  const wss = new EventEmitter();
  shell.attachTerminalWebSocket(wss, { verifyCsrf: (_req, token) => token === "csrf" });
  const preparationsBefore = fake.calls.filter((call) => call.project === "capture-error" && call.argv[3] === "reaper-prepare-session").length;
  const ptysBefore = fake.ptys.length;

  const ws = await attachFakeTerminal(wss, "capture-error", "error-1", { waitForReady: false });
  await waitFor(() => ws.readyState === 3);
  assert.equal(fake.calls.filter((call) => call.project === "capture-error" && call.argv[3] === "reaper-prepare-session").length, preparationsBefore);
  assert.equal(fake.ptys.length, ptysBefore + 1);
  assert.equal(ws.frames.some((frame) => frame.type === protocol.TYPES.OPENED), false);
  const errorFrame = ws.frames.find((frame) => frame.type === protocol.TYPES.PROTOCOL_ERROR);
  assert.equal(protocol.decodeJson(errorFrame.payload).message, "permission denied");
});

test("socket close during capture cannot spawn a late viewer", async () => {
  await createProject("cancel-attach");
  await shell.openProjectShell({ path: "cancel-attach", sessionName: "main" });
  const releaseCapture = fake.delayNextCapture("cancel-attach");
  const capturesBefore = fake.captureCompletionCount("cancel-attach");
  const ptysBefore = fake.ptys.length;
  const wss = new EventEmitter();
  shell.attachTerminalWebSocket(wss, { verifyCsrf: (_req, token) => token === "csrf" });

  const ws = await attachFakeTerminal(wss, "cancel-attach", "cancel-1", { waitForReady: false });
  await waitFor(() => fake.ptys.length === ptysBefore + 1 && fake.ptys.at(-1).writes.length === 3);
  ws.close(1000, "cancel");
  releaseCapture();
  await waitFor(() => fake.captureCompletionCount("cancel-attach") === capturesBefore + 1);
  await tick();
  assert.equal(fake.ptys.length, ptysBefore + 1);
  assert.equal(fake.ptys.at(-1).killed, true);
  assert.equal(ws.frames.some((frame) => frame.type === protocol.TYPES.OPENED), false);
});



test("control handoff parses split octal output and crosses the capture boundary exactly once", async () => {
  await createProject("atomic-control");
  await shell.openProjectShell({ path: "atomic-control", sessionName: "main" });
  fake.queueControlPlan("atomic-control", {
    modeChunks: ["%begin 2 1 0\n1,0,1,0,1,0,0,0,1,1\n%end 2 1 0\n"],
    captureChunks: [
      "%out",
      "put %1 SNAP\\040EDGE\n%beg",
      "in 2 2 0\nSNAP é",
      "\n%output %1 SNAP\\040EDGE\n%end 2 2",
      " 0\n%output %1 LIVE\\040\\000TAIL\n"
    ]
  });
  const wss = new EventEmitter();
  shell.attachTerminalWebSocket(wss, { verifyCsrf: (_req, token) => token === "csrf" });
  const ws = await attachFakeTerminal(wss, "atomic-control", "atomic-1");
  await waitFor(() => ws.frames.some((frame) => frame.type === protocol.TYPES.OUTPUT));
  const history = Buffer.concat(ws.frames.filter((frame) => frame.type === protocol.TYPES.HISTORY).map((frame) => Buffer.from(frame.payload)));
  const output = Buffer.concat(ws.frames.filter((frame) => frame.type === protocol.TYPES.OUTPUT).map((frame) => Buffer.from(frame.payload)));
  assert.deepEqual(
    fake.ptys.at(-1).writes.map((write) => write.toString().split(" ", 1)[0]),
    ["refresh-client", "display-message", "capture-pane"]
  );
  assert.equal(fake.ptys.at(-1).writes[2].toString().includes(" -E "), false);
  assert.ok(history.includes(Buffer.from("SNAP é")));
  assert.equal(history.subarray(-2).toString(), "é");
  assert.equal(history.toString().split("SNAP EDGE").length - 1, 0);
  assert.deepEqual(output, Buffer.from([0x4c, 0x49, 0x56, 0x45, 0x20, 0x00, 0x54, 0x41, 0x49, 0x4c]));
  assert.equal(output.includes(Buffer.from("SNAP EDGE")), false);
  assert.ok(history.indexOf(Buffer.from("\u001b[?1049h")) < history.indexOf(Buffer.from("SNAP é")));
  assert.ok(history.includes(Buffer.from("\u001b[?25l")));
  ws.close(1000, "done");
});

test("capture errors and early control PTY exits clean up without exposing READY", async () => {
  await createProject("early-control-exit");
  await shell.openProjectShell({ path: "early-control-exit", sessionName: "main" });
  fake.queueControlPlan("early-control-exit", {
    respond(pty) {
      pty.dataCallback("%begin 2 2 0\npartial\n");
      pty.exitCallback?.({ exitCode: 7 });
    }
  });
  const wss = new EventEmitter();
  shell.attachTerminalWebSocket(wss, { verifyCsrf: (_req, token) => token === "csrf" });
  const ws = await attachFakeTerminal(wss, "early-control-exit", "exit-1", { waitForReady: false });
  await waitFor(() => ws.readyState === 3);
  assert.equal(ws.frames.some((frame) => frame.type === protocol.TYPES.READY), false);
  assert.equal(fake.ptys.at(-1).killed, true);
});

test("malformed live control output deterministically closes and kills the control client", async () => {
  await createProject("malformed-control");
  await shell.openProjectShell({ path: "malformed-control", sessionName: "main" });
  const wss = new EventEmitter();
  shell.attachTerminalWebSocket(wss, { verifyCsrf: (_req, token) => token === "csrf" });
  const ws = await attachFakeTerminal(wss, "malformed-control", "malformed-1");
  const opened = ws.frames.find((frame) => frame.type === protocol.TYPES.OPENED);
  const pty = fake.ptys.at(-1);
  pty.dataCallback("%output %1 bad\\09\n");
  await waitFor(() => pty.killed);
  assert.ok(ws.frames.some((frame) => frame.type === protocol.TYPES.CLOSE_STREAM && frame.streamId === opened.streamId));
});

test("pathological capture retains bounded newest rows and resets before rendering", async () => {
  await createProject("bounded-control");
  await shell.openProjectShell({ path: "bounded-control", sessionName: "main" });
  fake.queueControlPlan("bounded-control", {
    respond(pty) {
      pty.dataCallback("%begin 2 2 0\n");
      const row = `${"x".repeat(65530)}\n`;
      for (let index = 0; index < 17; index += 1) pty.dataCallback(row);
      pty.dataCallback("newest-row\n%end 2 2 0\n");
    }
  });
  const wss = new EventEmitter();
  shell.attachTerminalWebSocket(wss, { verifyCsrf: (_req, token) => token === "csrf" });
  const ws = await attachFakeTerminal(wss, "bounded-control", "bounded-1");
  const history = Buffer.concat(ws.frames.filter((frame) => frame.type === protocol.TYPES.HISTORY).map((frame) => Buffer.from(frame.payload)));
  assert.ok(history.length <= 1024 * 1024 + 256);
  assert.equal(history.subarray(0, 2).toString("ascii"), "\u001bc");
  assert.ok(history.subarray(-12).toString().endsWith("newest-row"));
  ws.close(1000, "done");
});

test("published ports validate input and generate deterministic safe Caddy blocks", async () => {
  await createProject("ports-project");
  const reloadsBefore = fake.reloads;
  await assert.rejects(() => shell.updateProjectPorts("ports-project", [{ containerPort: 3000, subdomain: "bad.example\nattack" }]), /subdomain/);
  await assert.rejects(() => shell.updateProjectPorts("ports-project", [{ containerPort: 70000, subdomain: "app" }]), /containerPort/);
  const result = await shell.updateProjectPorts("ports-project", [
    { containerPort: 8080, subdomain: "web" },
    { containerPort: 3000, subdomain: "api" }
  ]);
  assert.deepEqual(result, {
    ports: [
      { containerPort: 3000, subdomain: "api" },
      { containerPort: 8080, subdomain: "web" }
    ],
    requireReaperAuth: true
  });
  assert.equal(await fs.readFile(caddyFile, "utf8"), `${expectedPublishedBlock("api", 3000)}\n\n${expectedPublishedBlock("web", 8080)}\n`);
  assert.equal(fake.reloads, reloadsBefore + 1);
});

test("published ports can explicitly disable Reaper auth and round-trip the setting", async () => {
  await createProject("public-ports-project");
  assert.deepEqual(await shell.getProjectPorts("public-ports-project"), { ports: [], requireReaperAuth: true });

  const result = await shell.updateProjectPorts(
    "public-ports-project",
    [{ containerPort: 4173, subdomain: "public-app" }],
    false
  );
  assert.deepEqual(result, {
    ports: [{ containerPort: 4173, subdomain: "public-app" }],
    requireReaperAuth: false
  });
  assert.deepEqual(await shell.getProjectPorts("public-ports-project"), result);
  assert.deepEqual(
    await shell.updateProjectPorts("public-ports-project", result.ports),
    result
  );
  const generated = await fs.readFile(caddyFile, "utf8");
  assert.match(generated, new RegExp(expectedPublishedBlock("public-app", 4173, false).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  assert.doesNotMatch(
    generated.match(/https:\/\/public-app\.example\.test \{[\s\S]*?\n\}/)?.[0] || "",
    /forward_auth/
  );
});

test("malformed publication auth settings are rejected fail-closed", async () => {
  await createProject("malformed-auth-project");
  await shell.getProjectPorts("malformed-auth-project");
  const trustedState = path.join(stateRoot, "projects", "malformed-auth-project");
  await fs.writeFile(path.join(trustedState, "publication.json"), JSON.stringify({ requireReaperAuth: "false" }));
  await assert.rejects(() => shell.getProjectPorts("malformed-auth-project"), /requireReaperAuth must be a boolean/);
  await fs.rm(path.join(trustedState, "publication.json"));
});

test("published subdomains are globally unique across projects", async () => {
  await createProject("ports-conflict");
  await assert.rejects(
    () => shell.updateProjectPorts("ports-conflict", [{ containerPort: 9000, subdomain: "web" }]),
    /already published by project "ports-project"/
  );
  assert.deepEqual((await shell.getProjectPorts("ports-conflict")).ports, []);
});

test("IP publishing binds the container port on the Reaper host", async () => {
  process.env.APEX_DOMAIN = "";
  process.env.REAPER_HOST = "167.86.121.124";
  try {
    await createProject("ip-ports-project");
    await assert.rejects(
      () => shell.updateProjectPorts("ip-ports-project", [{ containerPort: 80, subdomain: "http" }]),
      /available host port from 1024/
    );
    await shell.updateProjectPorts("ip-ports-project", [{ containerPort: 5173, subdomain: "app" }]);
    assert.match(await fs.readFile(caddyFile, "utf8"), new RegExp(expectedIpPublishedBlock(5173).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));

    await createProject("ip-port-conflict");
    await assert.rejects(
      () => shell.updateProjectPorts("ip-port-conflict", [{ containerPort: 5173, subdomain: "other" }]),
      /host port 5173 is already published by project "ip-ports-project"/
    );
  } finally {
    process.env.APEX_DOMAIN = "example.test";
    process.env.REAPER_HOST = "example.test";
  }
});

test("failed Caddy reload restores the prior published ports and auth setting", async () => {
  const before = await shell.getProjectPorts("ports-project");
  fake.failNextReload();
  await assert.rejects(
    () => shell.updateProjectPorts(
      "ports-project",
      [{ containerPort: 9000, subdomain: "replacement" }],
      false
    ),
    /simulated Caddy reload failure/
  );
  assert.deepEqual(await shell.getProjectPorts("ports-project"), before);
  assert.deepEqual(
    JSON.parse(await fs.readFile(path.join(stateRoot, "projects", "ports-project", "publication.json"), "utf8")),
    { requireReaperAuth: true }
  );
  const restored = await fs.readFile(caddyFile, "utf8");
  assert.match(restored, /https:\/\/api\.example\.test/);
  assert.match(restored, /forward_auth 127\.0\.0\.1:4000/);
  assert.doesNotMatch(restored, /replacement\.example\.test/);
});

test("project environment is validated, persisted, and applied to future tmux processes", async () => {
  await createProject("environment");
  await shell.openProjectShell({ path: "environment", sessionName: "main" });
  const applied = await shell.setProjectEnv("environment", { API_TOKEN: "quote' safe", EMPTY: "" });
  assert.equal(applied.count, 2);
  assert.equal(applied.sessions, 1);
  assert.deepEqual(
    JSON.parse(await fs.readFile(path.join(stateRoot, "projects", "environment", "env.json"), "utf8")),
    { API_TOKEN: "quote' safe", EMPTY: "" }
  );
  const bashrc = fake.installed.filter((item) => item.project === "environment" && item.target.endsWith("/rcfile")).at(-1)?.content || "";
  assert.match(bashrc, /export API_TOKEN='quote'"'"' safe'/);
  assert.match(bashrc, /export EMPTY=''/);
  const setCalls = fake.calls.filter((call) => call.project === "environment" && call.argv.includes("set-environment"));
  assert.ok(setCalls.some((call) => call.argv.includes("API_TOKEN") && call.argv.includes("quote' safe")));
  await assert.rejects(() => shell.setProjectEnv("environment", { "BAD-NAME": "x" }), /invalid environment variable name/);
});

test("no blanket context caps are injected without explicit configuration", async () => {
  await createProject("claude-context-defaults");
  await shell.openProjectShell({ path: "claude-context-defaults", sessionName: "main" });
  const bashrc = fake.installed
    .filter((item) => item.project === "claude-context-defaults" && item.target.endsWith("/rcfile"))
    .at(-1)?.content || "";
  assert.doesNotMatch(bashrc, /CLAUDE_CODE_MAX_CONTEXT_TOKENS/);
  assert.doesNotMatch(bashrc, /CLAUDE_CODE_AUTO_COMPACT_WINDOW/);
  assert.doesNotMatch(bashrc, /CLAUDE_AUTOCOMPACT_PCT_OVERRIDE/);
  assert.deepEqual({ ...(await shell.getProjectEnv("claude-context-defaults")) }, {});
});

test("per-project and global env caps apply only where explicitly set", async () => {
  shell.setGlobalEnvProvider(async () => ({
    CLAUDE_CODE_MAX_CONTEXT_TOKENS: "272000",
  }));
  try {
    await createProject("claude-context-overrides");
    await shell.openProjectShell({ path: "claude-context-overrides", sessionName: "main" });
    await shell.setProjectEnv("claude-context-overrides", {
      CLAUDE_CODE_AUTO_COMPACT_WINDOW: "220000",
      CLAUDE_AUTOCOMPACT_PCT_OVERRIDE: "90",
    });
    const bashrc = fake.installed
      .filter((item) => item.project === "claude-context-overrides" && item.target.endsWith("/rcfile"))
      .at(-1)?.content || "";
    assert.match(bashrc, /export CLAUDE_CODE_MAX_CONTEXT_TOKENS='272000'/);
    assert.match(bashrc, /export CLAUDE_CODE_AUTO_COMPACT_WINDOW='220000'/);
    assert.match(bashrc, /export CLAUDE_AUTOCOMPACT_PCT_OVERRIDE='90'/);
    assert.deepEqual(
      { ...(await shell.getProjectEnv("claude-context-overrides")) },
      { CLAUDE_CODE_AUTO_COMPACT_WINDOW: "220000", CLAUDE_AUTOCOMPACT_PCT_OVERRIDE: "90" }
    );
  } finally {
    shell.setGlobalEnvProvider(async () => Object.create(null));
  }
});

test("deleting a missing tmux process still removes its persistent session record", async () => {
  await createProject("missing-process");
  await shell.openProjectShell({ path: "missing-process", sessionName: "main" });
  fake.dropSession("missing-process", "main");
  const result = await shell.destroySession("main", { project: "missing-process" });
  assert.equal(result.ok, true);
  assert.deepEqual(await shell.listSessions({ path: "missing-process" }), []);
});

test("opening a terminal transport never creates a deleted session", async () => {
  await createProject("noncreating-open");
  await shell.openProjectShell({ path: "noncreating-open", sessionName: "main" });
  await shell.destroySession("main", { project: "noncreating-open" });
  await assert.rejects(
    () => shell.openProjectShell({ path: "noncreating-open", sessionName: "main", createIfMissing: false }),
    /session not found/
  );
  assert.deepEqual(await shell.listSessions({ path: "noncreating-open" }), []);
});

test("project deletion is serialized and queued terminal operations cannot recreate it", async () => {
  await createProject("delete-race");
  await shell.openProjectShell({ path: "delete-race", sessionName: "main" });
  const deletion = shell.destroyProjectRuntime("delete-race");
  const queuedOpen = shell.openProjectShell({ path: "delete-race", sessionName: "worker" })
    .then(() => null, (error) => error);
  const queuedList = shell.listSessions({ path: "delete-race" })
    .then(() => null, (error) => error);
  const deleted = await deletion;
  assert.equal(deleted.ok, true);
  assert.match((await queuedOpen)?.message || "", /project not found/);
  assert.match((await queuedList)?.message || "", /project not found/);
  await assert.rejects(fs.access(path.join(root, "delete-race")), (error) => error?.code === "ENOENT");
});

test("degraded subprocess resize is a no-op", () => {
  const degraded = shell.__createSubprocessSessionForTests();
  let writes = 0;
  degraded.write = () => { writes += 1; };
  degraded.resize(200, 60);
  assert.equal(writes, 0);
  assert.equal(degraded.degraded, true);
});

test.after(async () => {
  await shell.shutdownLocalShells();
  await fs.rm(root, { recursive: true, force: true });
});

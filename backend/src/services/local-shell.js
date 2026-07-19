import fs from "node:fs/promises";
import fss from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { StringDecoder } from "node:string_decoder";
import { isIP } from "node:net";
import { randomUUID } from "node:crypto";
import * as defaultPodRuntime from "./pod-runtime.js";
import {
  TYPES,
  FLAGS,
  DIRECTIONS,
  HEADER_SIZE,
  HISTORY_CHUNK_SIZE,
  OUTPUT_MAX_PAYLOAD,
  MAX_UNACKED_BYTES,
  MAX_BUFFERED_AMOUNT,
  BACKPRESSURE_TIMEOUT_MS,
  HEARTBEAT_MS,
  encodeFrame,
  decodeFrame,
  encodeJson,
  decodeJson,
  decodeResize,
} from "./terminal-protocol.js";

const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_DATA_ROOT = path.resolve(MODULE_DIR, "..", "..", "..", "..", "tmp");
const PROJECTS_ROOT = process.env.VPS_PROJECTS || path.join(DEFAULT_DATA_ROOT, "vps-projects");
const STATE_DIR = process.env.STATE_DIR || path.join(process.cwd(), ".reaper-local");
const SESSION_ARCHIVE_DIR = path.join(STATE_DIR, "archive");
const TMUX_SOCKET = "/reaper/tmux.sock";
const TMUX_CONFIG = "/reaper/tmux.conf";
const SESSION_NAME_RE = /^[a-z0-9-]{1,32}$/;
const DEFAULT_CLAUDE_CONTEXT_ENV = Object.freeze({});
const SUBDOMAIN_RE = /^(?!-)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const DOMAIN_RE = /^(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,63}$/;
const IP_PUBLISH_MIN_PORT = 1024;
const IP_PUBLISH_RESERVED_PORTS = new Set([2019, 4000]);
const MARKED_PROCESS_CLEANUP_SCRIPT = 'match=-Fx; [ "$2" = prefix ] && match=-F; for signal in TERM KILL; do matched=0; for environment in /proc/[0-9]*/environ; do [ -r "$environment" ] || continue; if grep -zq $match -- "$1" "$environment" 2>/dev/null; then matched=1; pid=${environment#/proc/}; pid=${pid%/environ}; [ "$pid" -le 1 ] || kill "-$signal" "$pid" 2>/dev/null || :; fi; done; if [ "$signal" = TERM ] && [ "$matched" = 1 ]; then sleep 0.3; fi; done; :';
const PREPARE_POD_SESSION_SCRIPT = [
  "set -eu",
  'socket=$1; config=$2; name=$3; target_log=$4; legacy_log=$5; session_command=$6; pipe_command=$7',
  'if ! tmux -S "$socket" has-session -t "=$name" 2>/dev/null; then',
  '  tmux -S "$socket" -f "$config" new-session -d -s "$name" -c /work "$session_command"',
  "fi",
  "mkdir -p -- /reaper/logs",
  'tmux -S "$socket" pipe-pane -t "=$name:" "$pipe_command"',
  'if [ -L "$legacy_log" ]; then',
  '  rm -f -- "$legacy_log"',
  'elif [ -f "$legacy_log" ]; then',
  '  cat -- "$legacy_log" >> "$target_log"',
  '  rm -f -- "$legacy_log"',
  "fi"
].join("\n");
const CAPTURE_POD_SESSION_SCRIPT = [
  "set -eu",
  'socket=$1; config=$2; name=$3; target_log=$4; legacy_log=$5; pipe_command=$6',
  "mkdir -p -- /reaper/logs",
  'if ! tmux -S "$socket" has-session -t "=$name" 2>/dev/null; then',
  '  printf "%s\n" REAPER_SESSION_MISSING >&2',
  "  exit 42",
  "fi",
  'tmux -S "$socket" pipe-pane -t "=$name:" "$pipe_command"',
  'if [ -L "$legacy_log" ]; then',
  '  rm -f -- "$legacy_log"',
  'elif [ -f "$legacy_log" ]; then',
  '  cat -- "$legacy_log" >> "$target_log"',
  '  rm -f -- "$legacy_log"',
  "fi",
  'exec tmux -S "$socket" -f "$config" -CC attach-session -t "=$name"'
].join("\n");
const CONTROL_HISTORY_LIMIT = 1024 * 1024;
const CONTROL_LINE_LIMIT = 256 * 1024;
const CONTROL_HISTORY_ROW_LIMIT = 16 * 1024;
const CONTROL_INGRESS_BYTES_PER_SECOND = 4 * 1024 * 1024;
const CONTROL_GLOBAL_INGRESS_BYTES_PER_SECOND = 32 * 1024 * 1024;
const CONTROL_DCS_PREFIX = Buffer.from("\u001bP1000p", "ascii");
const CONTROL_DCS_SUFFIX = Buffer.from("\u001b\\", "ascii");
const MAX_TERMINAL_COLS = 500;
const MAX_TERMINAL_ROWS = 300;
const MAX_STREAMS_PER_CONNECTION = 8;
const MAX_SESSIONS_PER_PROJECT = 32;
const MAX_PORTS_PER_PROJECT = 16;
const MAX_ENVIRONMENT_BYTES = 512 * 1024;
const MAX_GLOBAL_PUBLISHED_PORTS = 48;
const MAX_PENDING_OPENS_PER_CONNECTION = 2;
const MAX_ACTIVE_OPEN_OPERATIONS = 16;
const MAX_CONNECTIONS = 32;
const MAX_CONNECTIONS_PER_USER = 8;
const MAX_ACTIVE_STREAMS = 64;
const MAX_ACTIVE_STREAMS_PER_USER = 32;
const CONTROL_HANDOFF_TIMEOUT_MS = 10_000;
const MAX_PENDING_INPUT_BYTES = 64 * 1024;
const MAX_PENDING_INPUT_FRAMES = 128;
const MIN_CLIENT_PING_INTERVAL_MS = 250;
const MAX_CLIENT_FRAMES_PER_SECOND = 240;
const MAX_CLIENT_BYTES_PER_SECOND = 2 * 1024 * 1024;
const RESIZE_COALESCE_MS = 33;
const CONTROL_INPUT_FAST_PATH_BYTES = 4096;
const INPUT_BATCH_MAX_BYTES = 32 * 1024;
const CONTROL_MODE_FORMAT = [
  "alternate_on", "cursor_flag", "keypad_cursor_flag", "keypad_flag",
  "mouse_standard_flag", "mouse_button_flag", "mouse_any_flag",
  "mouse_utf8_flag", "mouse_sgr_flag", "bracket_paste_flag"
].map((name) => `#{?${name},1,0}`).join(",");
const EMPTY_BUFFER = Buffer.alloc(0);
const connections = new Set();
const connectionsByUser = new Map();
const activeStreamsByUser = new Map();
const viewersBySession = new Map();
const sessionActivity = new Map();
const fallbackSessions = new Map();
const stateMigrations = new Map();
const manifestOperations = new Map();
const manifestOperationDepth = new Map();
const viewerCleanupOperations = new Set();
let podRuntime = defaultPodRuntime;
let podMode = false;
let activeOpenOperations = 0;
let activeStreams = 0;
let caddyPortOperation = Promise.resolve();
let globalEnvProvider = async () => Object.create(null);
let controlIngressWindowAt = Date.now();
let controlIngressBytes = 0;

function nowIso() { return new Date().toISOString(); }
function projectRoot(project) { return path.join(PROJECTS_ROOT, project); }
function reaperRoot(project) { return path.join(STATE_DIR, "projects", project); }
function manifestPath(project) { return path.join(reaperRoot(project), "sessions.json"); }
function shellStateDir(project, name) { return path.join(reaperRoot(project), "shell-state", name); }
function sessionId(project, name) { return `${project}/${name}`; }
function podShellPath(name, file) { return `/reaper/shell-state/${name}/${file}`; }
function podLogPath(name) { return `/reaper/logs/${name}.log`; }
function shellQuote(value) { return `'${String(value).replace(/'/g, `'"'"'`)}'`; }

function assertProject(project) {
  try { defaultPodRuntime.validateProjectName(project); }
  catch { throw new Error("invalid project name"); }
  if (!fss.existsSync(projectRoot(project))) throw new Error("project not found");
  return project;
}

function normalizeSessionName(value = "main") {
  const name = String(value || "main").trim().toLowerCase();
  if (!SESSION_NAME_RE.test(name)) throw new Error("session name must match [a-z0-9-]{1,32}");
  return name;
}

function normalizeTitle(value, name) {
  const title = String(value ?? name).trim().replace(/\s+/g, " ");
  if (!title) return name;
  if (title.length > 80 || /[\u0000-\u001f\u007f]/.test(title)) throw new Error("session title is invalid");
  return title;
}
function validateProjectBashrc(content) {
  if (typeof content !== "string" || content.includes("\0") || Buffer.byteLength(content) > 256 * 1024) {
    throw new TypeError("bashrc must be UTF-8 text no larger than 256 KiB");
  }
  return content;
}


function normalizeProjectEnv(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new TypeError("project environment must be an object");
  const entries = Object.entries(value);
  if (entries.length > 256) throw new TypeError("project environment cannot exceed 256 variables");
  const clean = Object.create(null);
  let totalBytes = 2;
  for (const [key, rawValue] of entries) {
    if (key.length > 128 || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) throw new TypeError(`invalid environment variable name: ${key}`);
    if (typeof rawValue !== "string" || rawValue.includes("\0") || Buffer.byteLength(rawValue) > 65_536) {
      throw new TypeError(`invalid value for environment variable: ${key}`);
    }
    totalBytes += Buffer.byteLength(key) + Buffer.byteLength(rawValue) + 6;
    if (totalBytes > MAX_ENVIRONMENT_BYTES) {
      throw new TypeError(`environment cannot exceed ${MAX_ENVIRONMENT_BYTES} serialized bytes`);
    }
    clean[key] = rawValue;
  }
  return clean;
}

function validateShellEnvironment(value) {
  return normalizeProjectEnv(value);
}

function setGlobalEnvProvider(provider) {
  if (typeof provider !== "function") throw new TypeError("global environment provider must be a function");
  globalEnvProvider = provider;
}

async function readGlobalShellEnv() {
  return normalizeProjectEnv(await globalEnvProvider());
}

function mergeShellEnvironment(globalEnv, projectEnv) {
  return normalizeProjectEnv(Object.assign(
    Object.create(null),
    DEFAULT_CLAUDE_CONTEXT_ENV,
    globalEnv,
    projectEnv
  ));
}

async function applyTmuxEnvironment(project, previous, next) {
  if (!podMode) return 0;
  const info = await podRuntime.podInspect(project);
  if (!info?.running) return 0;
  let count = 0;
  for (const key of new Set([...Object.keys(previous), ...Object.keys(next)])) {
    if (previous[key] === next[key] && Object.hasOwn(previous, key) === Object.hasOwn(next, key)) continue;
    const argv = Object.hasOwn(next, key)
      ? ["tmux", "-S", TMUX_SOCKET, "set-environment", "-g", key, next[key]]
      : ["tmux", "-S", TMUX_SOCKET, "set-environment", "-gu", key];
    const result = await podRuntime.podExec(project, argv);
    if (result.code !== 0 && !tmuxSessionIsAbsent(result)) {
      throw new Error(result.stderr || `failed to update tmux environment variable ${key}`);
    }
    if (result.code === 0) count += 1;
  }
  return count;
}

async function readProjectEnv(project) {
  assertProject(project);
  await ensureTrustedProjectState(project);
  try {
    return normalizeProjectEnv(JSON.parse(await fs.readFile(path.join(reaperRoot(project), "env.json"), "utf8")));
  } catch (error) {
    if (error.code === "ENOENT") return Object.create(null);
    throw error;
  }
}

async function atomicJson(file, value) {
  await fs.mkdir(path.dirname(file), { recursive: true, mode: 0o700 });
  const temporary = `${file}.tmp-${process.pid}-${Date.now()}`;
  try {
    await fs.writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { encoding: "utf8", mode: 0o600 });
    await fs.rename(temporary, file);
  } catch (error) {
    await fs.rm(temporary, { force: true });
    throw error;
  }
}

function controlMarkerPath(project) {
  return path.join(reaperRoot(project), "control.json");
}

async function readLegacyPodFile(project, file, maxBuffer) {
  const result = await podRuntime.podExec(project, ["timeout", "2", "cat", "--", file], { maxBuffer });
  if (result.code === 0) return result.stdout;
  const detail = result.stderr.trim() || result.stdout.trim();
  if (result.code === 1 && /no such file or directory/i.test(detail)) return null;
  throw new Error(`failed to read legacy project file ${file}: ${detail || `exit code ${result.code}`}`);
}

async function migrateLegacyProjectState(project) {
  if (!podMode) {
    await atomicJson(controlMarkerPath(project), { version: 1 });
    return;
  }
  await podRuntime.ensurePod(project, projectRoot(project));
  const legacyManifest = await readLegacyPodFile(project, "/work/.reaper/sessions.json", 1024 * 1024);
  let explicitManifest = false;
  const byName = new Map();
  if (legacyManifest !== null) {
    let parsed;
    try {
      parsed = JSON.parse(legacyManifest);
    } catch (error) {
      throw new Error(`invalid legacy sessions manifest: ${error.message}`);
    }
    if (!Array.isArray(parsed)) throw new Error("invalid legacy sessions manifest: expected an array");
    if (parsed.length > MAX_SESSIONS_PER_PROJECT) {
      throw new Error(`invalid legacy sessions manifest: cannot exceed ${MAX_SESSIONS_PER_PROJECT} sessions`);
    }
    explicitManifest = true;
    for (const item of parsed) {
      const name = normalizeSessionName(item?.name);
      if (!byName.has(name)) {
        byName.set(name, {
          name,
          title: normalizeTitle(item?.title, name),
          createdAt: typeof item?.createdAt === "string" ? item.createdAt : nowIso()
        });
      }
    }
  }
  if (byName.size > MAX_SESSIONS_PER_PROJECT) {
    throw new Error(`invalid legacy sessions manifest: cannot exceed ${MAX_SESSIONS_PER_PROJECT} sessions`);
  }
  const running = await podRuntime.podExec(project, ["tmux", "-S", TMUX_SOCKET, "list-sessions", "-F", "#{session_name}"]);
  if (running.code !== 0) {
    const detail = running.stderr.trim() || running.stdout.trim();
    if (!/(?:no server running|failed to connect to server)/i.test(detail)) {
      throw new Error(`failed to inspect legacy tmux sessions: ${detail || `exit code ${running.code}`}`);
    }
  } else {
    const runningNames = running.stdout.split(/\r?\n/).filter(Boolean);
    if (runningNames.length > MAX_SESSIONS_PER_PROJECT) {
      throw new Error(`legacy tmux session count cannot exceed ${MAX_SESSIONS_PER_PROJECT}`);
    }
    for (const rawName of runningNames) {
      const name = normalizeSessionName(rawName);
      if (!byName.has(name)) byName.set(name, { name, title: name, createdAt: nowIso() });
      if (byName.size > MAX_SESSIONS_PER_PROJECT) {
        throw new Error(`legacy session count cannot exceed ${MAX_SESSIONS_PER_PROJECT}`);
      }
    }
  }
  if (!byName.size && !explicitManifest) {
    byName.set("main", { name: "main", title: "main", createdAt: nowIso() });
  }

  let cleanEnv = Object.create(null);
  const legacyEnv = await readLegacyPodFile(project, "/work/.reaper/env.json", 1024 * 1024);
  if (legacyEnv !== null) {
    try {
      cleanEnv = normalizeProjectEnv(JSON.parse(legacyEnv));
    } catch (error) {
      throw new Error(`invalid legacy project environment: ${error.message}`);
    }
  }

  let bashrc = await readLegacyPodFile(project, "/work/.reaper/bashrc", 512 * 1024);
  if (bashrc === null) bashrc = "";
  else if (bashrc.includes("\0") || Buffer.byteLength(bashrc) > 256 * 1024) {
    throw new Error("invalid legacy project bashrc");
  }

  let cleanPorts = [];
  const legacyPorts = await readLegacyPodFile(project, "/work/.reaper/ports.json", 1024 * 1024);
  if (legacyPorts !== null) {
    try {
      cleanPorts = validatePorts(JSON.parse(legacyPorts));
    } catch (error) {
      throw new Error(`invalid legacy published ports: ${error.message}`);
    }
  }

  await atomicJson(manifestPath(project), [...byName.values()]);
  await atomicJson(path.join(reaperRoot(project), "env.json"), cleanEnv);
  await atomicText(path.join(reaperRoot(project), "bashrc"), bashrc);
  await atomicJson(path.join(reaperRoot(project), "ports.json"), cleanPorts);
  await atomicJson(controlMarkerPath(project), { version: 1, migratedAt: nowIso() });
}

async function ensureTrustedProjectState(project) {
  try {
    await fs.access(controlMarkerPath(project));
    return;
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  const previous = stateMigrations.get(project);
  if (previous) return previous;
  const operation = migrateLegacyProjectState(project);
  stateMigrations.set(project, operation);
  try {
    await operation;
  } finally {
    if (stateMigrations.get(project) === operation) stateMigrations.delete(project);
  }
}

function serializeManifest(project, work) {
  const depth = manifestOperationDepth.get(project) || 0;
  if (depth >= 4) {
    const error = new Error("project operation queue is busy");
    error.statusCode = 429;
    return Promise.reject(error);
  }
  manifestOperationDepth.set(project, depth + 1);
  const previous = manifestOperations.get(project) || Promise.resolve();
  const operation = previous.catch(() => {}).then(work);
  manifestOperations.set(project, operation);
  operation.finally(() => {
    const remaining = Math.max(0, (manifestOperationDepth.get(project) || 1) - 1);
    if (remaining) manifestOperationDepth.set(project, remaining);
    else manifestOperationDepth.delete(project);
    if (manifestOperations.get(project) === operation) manifestOperations.delete(project);
  }).catch(() => {});
  return operation;
}

async function readManifest(project, { createDefault = true } = {}) {
  assertProject(project);
  await ensureTrustedProjectState(project);
  let entries;
  let missing = false;
  try {
    entries = JSON.parse(await fs.readFile(manifestPath(project), "utf8"));
  } catch (error) {
    if (error.code !== "ENOENT") throw new Error(`invalid sessions manifest: ${error.message}`);
    entries = [];
    missing = true;
  }
  if (!Array.isArray(entries)) throw new Error("invalid sessions manifest");
  if (entries.length > MAX_SESSIONS_PER_PROJECT) {
    throw new Error(`sessions manifest cannot exceed ${MAX_SESSIONS_PER_PROJECT} sessions`);
  }
  const seenNames = new Set();
  const clean = entries.map((entry) => {
    const name = normalizeSessionName(entry?.name);
    if (seenNames.has(name)) throw new Error(`sessions manifest contains duplicate session: ${name}`);
    seenNames.add(name);
    return {
      name,
      title: normalizeTitle(entry?.title, name),
      createdAt: typeof entry?.createdAt === "string" ? entry.createdAt : nowIso(),
      ...(entry?.interrupted && typeof entry.interrupted === "object" ? { interrupted: entry.interrupted } : {})
    };
  });
  if (createDefault && missing) {
    clean.push({ name: "main", title: "main", createdAt: nowIso() });
    await atomicJson(manifestPath(project), clean);
  }
  return clean;
}

async function writeManifest(project, entries) {
  if (!Array.isArray(entries) || entries.length > MAX_SESSIONS_PER_PROJECT) {
    throw new Error(`sessions manifest cannot exceed ${MAX_SESSIONS_PER_PROJECT} sessions`);
  }
  await atomicJson(manifestPath(project), entries);
}

function stripTerminalQueries(text) {
  return String(text)
    .replace(/\x1b\[(?:5|6|\?6)n/g, "")
    .replace(/\x1b\[(?:0|>|>0|=)?c/g, "")
    .replace(/\x1bP(?:\$q|\+q)[\s\S]*?\x1b\\/g, "")
    .replace(/\x1b\]4;\d+;\?(?:\x07|\x1b\\)/g, "")
    .replace(/\x1b\](?:10|11|12);\?(?:\x07|\x1b\\)/g, "")
    .replace(/\x1b\[\?u/g, "")
    .replace(/\x1b\[\?\d+\$p/g, "");
}

async function readProjectBashrc(project) {
  assertProject(project);
  await ensureTrustedProjectState(project);
  try { return await fs.readFile(path.join(reaperRoot(project), "bashrc"), "utf8"); }
  catch (error) {
    if (error.code === "ENOENT") return "";
    throw error;
  }
}

async function atomicText(file, content, mode = 0o600) {
  await fs.mkdir(path.dirname(file), { recursive: true, mode: 0o700 });
  const temporary = `${file}.tmp-${process.pid}-${Date.now()}`;
  try {
    await fs.writeFile(temporary, content, { encoding: "utf8", mode });
    await fs.rename(temporary, file);
  } catch (error) {
    await fs.rm(temporary, { force: true });
    throw error;
  }
}

async function installPodText(project, target, content) {
  const installed = await podRuntime.podExec(project, [
    "sh", "-c",
    'set -eu; target=$1; directory=${target%/*}; mkdir -p -- "$directory"; chmod 700 "$directory"; temporary="${target}.tmp.$$"; trap \'rm -f -- "$temporary"\' EXIT; cat > "$temporary"; chmod 600 "$temporary"; mv -f -- "$temporary" "$target"; trap - EXIT',
    "reaper-install", target
  ], { input: content });
  if (installed.code !== 0) throw new Error(installed.stderr || `failed to install ${target}`);
}

async function ensureShellRecoveryFiles(project, name, shellConfig = {}) {
  const projectEnv = shellConfig.shellEnv || mergeShellEnvironment(await readGlobalShellEnv(), await readProjectEnv(project));
  const projectBashrc = shellConfig.projectBashrc ?? await readProjectBashrc(project);
  const runtimeFile = (file) => podMode ? podShellPath(name, file) : path.join(shellStateDir(project, name), file);
  const state = shellQuote(runtimeFile("state.sh"));
  const history = shellQuote(runtimeFile("history"));
  const interrupted = shellQuote(runtimeFile("interrupted"));
  const rc = [
    "# Generated by Reaper. Shell state is checkpointed after each prompt.",
    "if [ -f /root/.bashrc ]; then . /root/.bashrc; fi",
    "# Project shell configuration.",
    projectBashrc,
    ...Object.entries(projectEnv).map(([key, value]) => `export ${key}=${shellQuote(value)}`),
    `if [ -r ${state} ]; then . ${state}; fi`,
    `export HISTFILE=${history}`,
    "export HISTSIZE=${HISTSIZE:-50000}",
    "export HISTFILESIZE=${HISTFILESIZE:-50000}",
    "history -r \"$HISTFILE\" 2>/dev/null || true",
    "declare -gA __reaper_base_exports=()",
    "while IFS= read -r __line; do __rest=\"${__line#declare -x }\"; __name=\"${__rest%%=*}\"; __reaper_base_exports[\"$__name\"]=\"$__line\"; done < <(export -p)",
    "__reaper_checkpoint() {",
    `  local __state=${state} __tmp=${state}.tmp.$$ __line __rest __name`,
    "  history -a 2>/dev/null || true",
    "  {",
    "    printf 'if [ -d %q ]; then cd -- %q; fi\\n' \"$PWD\" \"$PWD\"",
    "    alias -p || true",
    "    while IFS= read -r __line; do",
    "      __rest=\"${__line#declare -x }\"; __name=\"${__rest%%=*}\"",
    "      case \"$__name\" in PWD|OLDPWD|SHLVL|_|TERM|TMUX|TMUX_PANE|HOSTNAME|HISTFILE) continue ;; esac",
    "      if [[ ${__reaper_base_exports[$__name]+_} && \"${__reaper_base_exports[$__name]}\" == \"$__line\" ]]; then continue; fi",
    "      printf '%s\\n' \"$__line\"",
    "    done < <(export -p)",
    "  } > \"$__tmp\" && chmod 600 \"$__tmp\" && mv -f \"$__tmp\" \"$__state\"",
    "}",
    "case \";${PROMPT_COMMAND:-};\" in *';__reaper_checkpoint;'*) ;; *) PROMPT_COMMAND=\"__reaper_checkpoint${PROMPT_COMMAND:+;$PROMPT_COMMAND}\" ;; esac",
    "__reaper_mark() {",
    `  printf '%s\\n%s\\n%s\\n' \"$1\" \"$PWD\" \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\" > ${interrupted}`,
    "}",
    `opencode() { __reaper_mark opencode; command opencode "$@"; local __status=$?; rm -f ${interrupted}; return "$__status"; }`,
    `omp() { __reaper_mark omp; command omp "$@"; local __status=$?; rm -f ${interrupted}; return "$__status"; }`,
    "reaper-resume() {",
    `  if [ ! -r ${interrupted} ]; then printf 'Reaper: no interrupted supported command.\\n'; return 1; fi`,
    `  local __cmd __cwd; __cmd=$(sed -n '1p' ${interrupted}); __cwd=$(sed -n '2p' ${interrupted})`,
    "  if [ -d \"$__cwd\" ]; then cd -- \"$__cwd\"; fi",
    `  local __status; case "$__cmd" in opencode) command opencode --continue ;; omp) command omp --continue ;; *) printf 'Reaper: cannot resume %s automatically.\\n' "$__cmd" >&2; return 2 ;; esac; __status=$?; rm -f ${interrupted}; return "$__status"`,
    "}",
    `if [ -r ${interrupted} ]; then`,
    `  __cmd=$(sed -n '1p' ${interrupted}); __at=$(sed -n '3p' ${interrupted})`,
    "  printf '\\033[38;5;214mReaper: %s was interrupted at %s and was not restarted. Run reaper-resume to opt in.\\033[0m\\n' \"$__cmd\" \"$__at\"",
    "  unset __cmd __at",
    "fi",
    "__reaper_checkpoint"
  ].join("\n") + "\n";
  if (podMode) {
    const target = podShellPath(name, "rcfile");
    await installPodText(project, target, rc);
    return target;
  }
  const rcPath = path.join(shellStateDir(project, name), "rcfile");
  await atomicText(rcPath, rc);
  return rcPath;
}

async function ensurePodSession(project, entry, { podReady = false, shellConfig } = {}) {
  if (!podReady) await podRuntime.ensurePod(project, projectRoot(project));
  const rc = await ensureShellRecoveryFiles(project, entry.name, shellConfig);
  const targetLog = podLogPath(entry.name);
  const prepared = await podRuntime.podExec(project, [
    "sh", "-c", PREPARE_POD_SESSION_SCRIPT,
    "reaper-prepare-session",
    TMUX_SOCKET,
    TMUX_CONFIG,
    entry.name,
    targetLog,
    `/work/.reaper/logs/${entry.name}.log`,
    `/usr/local/bin/reaper-session ${shellQuote(rc)} ${shellQuote(entry.name)}`,
    `cat >> ${shellQuote(targetLog)}`
  ]);
  if (prepared.code !== 0) throw new Error(prepared.stderr || `failed to prepare session ${entry.name}`);
}

async function syncPodManifest(project, entries, { podReady = false } = {}) {
  if (!podMode) return;
  if (!podReady) {
    const info = await podRuntime.podInspect(project);
    if (!info?.running) return;
  }
  await installPodText(project, "/work/.reaper/sessions.json", `${JSON.stringify(entries, null, 2)}\n`);
}

async function preparePodSessions(project, sessions, entries = sessions, { podReady = false, shellConfig = null } = {}) {
  if (!sessions.length) {
    await syncPodManifest(project, entries, { podReady });
    return;
  }
  if (!podReady) await podRuntime.ensurePod(project, projectRoot(project));
  shellConfig ||= {
    shellEnv: mergeShellEnvironment(await readGlobalShellEnv(), await readProjectEnv(project)),
    projectBashrc: await readProjectBashrc(project)
  };
  for (const entry of sessions) await ensurePodSession(project, entry, { podReady: true, shellConfig });
  await syncPodManifest(project, entries, { podReady: true });
}

class SubprocessSession {
  constructor(project, entry) {
    this.project = project;
    this.entry = entry;
    this.viewers = new Set();
    this.proc = null;
    this.alive = false;
    this.degraded = true;
  }
  start() {
    if (this.proc) return;
    const windows = process.platform === "win32";
    const command = windows ? "cmd.exe" : "/bin/sh";
    const args = windows ? ["/Q", "/K"] : ["-i"];
    this.proc = spawn(command, args, { cwd: projectRoot(this.project), env: { ...process.env, REAPER_PROJECT: this.project, REAPER_SESSION: this.entry.name, TERM: "xterm-256color" }, stdio: ["pipe", "pipe", "pipe"], windowsHide: true });
    this.alive = true;
    const output = (chunk) => { for (const viewer of this.viewers) viewer.onData?.(chunk); };
    this.proc.stdout.on("data", output);
    this.proc.stderr.on("data", output);
    this.proc.on("exit", (code) => { this.alive = false; this.proc = null; for (const viewer of this.viewers) viewer.onExit?.(code); });
  }
  attach(viewer) { this.viewers.add(viewer); this.start(); }
  detach(viewer) { this.viewers.delete(viewer); }
  write(data) { return Boolean(this.proc?.stdin?.writable && this.proc.stdin.write(data)); }
  resize() { /* Degraded pipes are not PTYs; writing a resize escape corrupts input. */ }
  destroy() { try { this.proc?.kill(); } catch {} this.proc = null; this.alive = false; }
}

async function selectBackend() {
  const forceSubprocess = String(process.env.REAPER_FORCE_BACKEND || "").toLowerCase() === "subprocess";
  if (forceSubprocess) {
    if (process.env.NODE_ENV === "production") throw new Error("REAPER_FORCE_BACKEND=subprocess is forbidden in production");
    podMode = false;
    return false;
  }
  const available = await podRuntime.podAvailable();
  podMode = Boolean(available?.available);
  if (!podMode && process.env.NODE_ENV === "production") {
    throw new Error(`project pod runtime unavailable: ${available?.reason || "Docker daemon unavailable"}`);
  }
  return podMode;
}

async function openProjectShell({ path: project, sessionName = "main", name, title, cols = 120, rows = 32, createIfMissing = true, bashrc } = {}) {
  assertProject(project);
  const requested = normalizeSessionName(name || sessionName);
  return serializeManifest(project, async () => {
    assertProject(project);
    const entries = await readManifest(project);
    let entry = entries.find((item) => item.name === requested);
    let created = false;
    if (!entry && !createIfMissing) throw new Error("session not found");
    if (!entry) {
      if (entries.length >= MAX_SESSIONS_PER_PROJECT) {
        throw new Error(`a project cannot exceed ${MAX_SESSIONS_PER_PROJECT} persistent sessions`);
      }
      entry = { name: requested, title: normalizeTitle(title, requested), createdAt: nowIso() };
      entries.push(entry);
      await writeManifest(project, entries);
      created = true;
    } else if (title !== undefined && normalizeTitle(title, requested) !== entry.title) {
      throw new Error("session already exists");
    }
    if (bashrc !== undefined) await atomicText(path.join(reaperRoot(project), "bashrc"), validateProjectBashrc(bashrc));
    if (podMode) {
      await preparePodSessions(project, [entry], entries);
    } else {
      const id = sessionId(project, requested);
      if (!fallbackSessions.has(id)) fallbackSessions.set(id, new SubprocessSession(project, entry));
    }
    if (created) broadcastSessionEvent("created", project, entry);
    return { session: presentSession(project, entry, podMode ? "running" : "degraded"), runtime: podMode ? null : fallbackSessions.get(sessionId(project, requested)), cols, rows };
  });
}
async function readExistingSessionEntry(project, name) {
  return serializeManifest(project, async () => {
    assertProject(project);
    const entry = (await readManifest(project, { createDefault: false })).find((item) => item.name === name);
    if (!entry) throw new Error("session not found");
    return Object.freeze({ ...entry });
  });
}


function presentSession(project, entry, state = "configured") {
  const id = sessionId(project, entry.name);
  const activity = sessionActivity.get(id);
  return {
    sessionId: id,
    name: entry.name,
    title: entry.title,
    path: project,
    project,
    persistent: true,
    destroyable: true,
    state,
    createdAt: entry.createdAt,
    attachedClients: viewersBySession.get(id)?.size || 0,
    ...(activity?.lastInteractionAt ? { lastInteractionAt: activity.lastInteractionAt } : {}),
    ...(entry.interrupted ? { interrupted: entry.interrupted } : {})
  };
}

async function listSessions({ path: project } = {}) {
  assertProject(project);
  return serializeManifest(project, async () => {
    assertProject(project);
    const entries = await readManifest(project);
    let names = new Set();
    if (podMode) {
      const result = await podRuntime.podExec(project, ["tmux", "-S", TMUX_SOCKET, "list-sessions", "-F", "#{session_name}"]).catch(() => ({ code: 1, stdout: "" }));
      if (result.code === 0) names = new Set(result.stdout.split(/\r?\n/).filter(Boolean));
    }
    return entries.map((entry) => presentSession(project, entry, podMode ? (names.has(entry.name) ? "running" : "configured") : (fallbackSessions.get(sessionId(project, entry.name))?.alive ? "degraded" : "configured")));
  });
}

async function renameSession(project, name, title) {
  assertProject(project);
  name = normalizeSessionName(name);
  return serializeManifest(project, async () => {
    assertProject(project);
    const entries = await readManifest(project);
    const entry = entries.find((item) => item.name === name);
    if (!entry) return { ok: false, error: "session not found" };
    entry.title = normalizeTitle(title, name);
    await writeManifest(project, entries);
    await syncPodManifest(project, entries);
    const activeViewers = viewersBySession.get(sessionId(project, name));
    if (activeViewers) {
      const currentEntry = Object.freeze({ ...entry });
      for (const viewer of activeViewers) viewer.entry = currentEntry;
    }
    broadcastSessionEvent("updated", project, entry);
    return { ok: true, session: presentSession(project, entry, podMode ? "running" : "configured") };
  });
}

async function archiveSessionLog(project, name, { remove = true } = {}) {
  let content;
  let source;
  if (podMode) {
    source = podLogPath(name);
    const captured = await podRuntime.podExec(project, [
      "sh", "-c",
      'if [ ! -f "$1" ] || [ -L "$1" ]; then exit 44; fi; timeout 5 tail -c 50331648 -- "$1"',
      "reaper-archive", source
    ], { maxBuffer: 64 * 1024 * 1024 });
    if (captured.code === 44 || /no such container/i.test(captured.stderr || "")) return null;
    if (captured.code !== 0) throw new Error(captured.stderr || `failed to archive session ${name}`);
    content = captured.stdout;
  } else {
    source = path.join(reaperRoot(project), "logs", `${name}.log`);
    try { content = await fs.readFile(source, "utf8"); }
    catch (error) {
      if (error.code === "ENOENT") return null;
      throw error;
    }
  }
  const directory = path.join(SESSION_ARCHIVE_DIR, "projects", project);
  const destination = path.join(directory, `${name}-${Date.now()}.log`);
  await atomicText(destination, content);
  if (remove && podMode) {
    const removed = await podRuntime.podExec(project, ["rm", "-f", "--", source]);
    if (removed.code !== 0 && !/no such container/i.test(removed.stderr || "")) {
      throw new Error(removed.stderr || `failed to remove archived session log ${name}`);
    }
  } else if (remove) {
    await fs.rm(source, { force: true });
  }
  return destination;
}

function tmuxSessionIsAbsent(result) {
  return result.code !== 0 && /(?:can't find session|no server running|no such container)/i.test(result.stderr || result.stdout || "");
}

async function terminateMarkedProcesses(project, marker, matchMode, description) {
  const terminated = await podRuntime.podExec(project, [
    "sh", "-c", MARKED_PROCESS_CLEANUP_SCRIPT,
    "reaper-process-cleanup", marker, matchMode
  ]);
  if (terminated.code !== 0 && !tmuxSessionIsAbsent(terminated)) {
    throw new Error(terminated.stderr || `failed to terminate ${description}`);
  }
}

async function terminateMarkedSessionProcesses(project, name) {
  return terminateMarkedProcesses(project, `REAPER_SESSION_ID=${name}`, "exact", `processes for session ${name}`);
}

async function detachOrphanViewerProcesses(project) {
  const socket = shellQuote(TMUX_SOCKET);
  const detached = await podRuntime.podExec(project, [
    "sh", "-c",
    `tmux -S ${socket} list-clients -F '#{client_tty}' 2>/dev/null | while IFS= read -r client; do [ -n "$client" ] && tmux -S ${socket} detach-client -t "$client" 2>/dev/null || :; done; :`,
    "reaper-viewer-prune"
  ]);
  if (detached.code !== 0 && !tmuxSessionIsAbsent(detached)) {
    throw new Error(detached.stderr || "failed to detach orphan terminal viewers");
  }
  await terminateMarkedProcesses(project, "REAPER_VIEWER_ID=", "prefix", "orphan terminal viewers");
}

async function destroySessionUnlocked(project, name, { processesAlreadyStopped = false } = {}) {
  const entries = await readManifest(project, { createDefault: false });
  const entry = entries.find((item) => item.name === name);
  if (!entry) return { ok: false, error: "session not found" };
  if (podMode && !processesAlreadyStopped) {
    const killed = await podRuntime.podExec(project, ["tmux", "-S", TMUX_SOCKET, "kill-session", "-t", `=${name}`]);
    if (killed.code !== 0 && !tmuxSessionIsAbsent(killed)) throw new Error(killed.stderr || `failed to stop session ${name}`);
    await terminateMarkedSessionProcesses(project, name);
  } else if (!podMode) {
    fallbackSessions.get(sessionId(project, name))?.destroy();
    fallbackSessions.delete(sessionId(project, name));
  }
  detachSessionViewers(project, name, "SESSION_DELETED");
  let archivedLog = null;
  try {
    archivedLog = await archiveSessionLog(project, name);
  } catch (error) {
    console.error(`[reaper] failed to archive deleted session ${project}/${name}:`, error.message);
  }
  if (podMode) {
    const cleaned = await podRuntime.podExec(project, ["rm", "-rf", "--", podShellPath(name, ""), `/work/.reaper/shell-state/${name}`]);
    if (cleaned.code !== 0 && !tmuxSessionIsAbsent(cleaned)) {
      console.error(`[reaper] failed to remove deleted session state ${project}/${name}:`, cleaned.stderr || `exit code ${cleaned.code}`);
    }
  } else {
    await fs.rm(shellStateDir(project, name), { recursive: true, force: true });
  }
  const remaining = entries.filter((item) => item.name !== name);
  await writeManifest(project, remaining);
  await syncPodManifest(project, remaining);
  sessionActivity.delete(sessionId(project, name));
  broadcastSessionEvent("deleted", project, entry);
  return { ok: true, archivedLog };
}

async function destroySession(idOrName, { project } = {}) {
  let name = String(idOrName || "");
  if (name.includes("/")) {
    const split = name.indexOf("/");
    project ||= name.slice(0, split);
    name = name.slice(split + 1);
  }
  assertProject(project);
  name = normalizeSessionName(name);
  return serializeManifest(project, () => {
    assertProject(project);
    return destroySessionUnlocked(project, name);
  });
}


async function setProjectEnv(project, env) {
  assertProject(project);
  const clean = normalizeProjectEnv(env);
  return serializeManifest(project, async () => {
    assertProject(project);
    const globalEnv = await readGlobalShellEnv();
    const previous = await readProjectEnv(project);
    const previousEffective = mergeShellEnvironment(globalEnv, previous);
    const nextEffective = mergeShellEnvironment(globalEnv, clean);
    const entries = await readManifest(project);
    await atomicJson(path.join(reaperRoot(project), "env.json"), clean);
    try {
      if (podMode) await preparePodSessions(project, entries, entries);
      else for (const entry of entries) await ensureShellRecoveryFiles(project, entry.name);
      const count = await applyTmuxEnvironment(project, previousEffective, nextEffective);
      return { count, sessions: entries.length };
    } catch (error) {
      try {
        await atomicJson(path.join(reaperRoot(project), "env.json"), previous);
        if (podMode) await preparePodSessions(project, entries, entries);
        else for (const entry of entries) await ensureShellRecoveryFiles(project, entry.name);
        await applyTmuxEnvironment(project, nextEffective, previousEffective);
      } catch (rollbackError) {
        throw new AggregateError([error, rollbackError], "project environment update failed and rollback could not restore shells");
      }
      throw error;
    }
  });
}

async function getProjectEnv(project) {
  assertProject(project);
  return serializeManifest(project, () => {
    assertProject(project);
    return readProjectEnv(project);
  });
}

async function refreshGlobalEnvironment(previousValue, nextValue, isCurrent = () => true) {
  const previousGlobal = normalizeProjectEnv(previousValue);
  const nextGlobal = normalizeProjectEnv(nextValue);
  let projectEntries = [];
  try {
    projectEntries = await fs.readdir(PROJECTS_ROOT, { withFileTypes: true });
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  const projects = projectEntries
    .filter((entry) => entry.isDirectory() && !entry.name.startsWith("."))
    .map((entry) => entry.name)
    .sort();
  let nextProject = 0;
  let count = 0;
  let sessions = 0;
  const failures = [];
  const worker = async () => {
    while (nextProject < projects.length && isCurrent()) {
      const project = projects[nextProject++];
      try {
        const result = await serializeManifest(project, async () => {
          assertProject(project);
          if (!isCurrent()) return { count: 0, sessions: 0, stale: true };
          const projectEnv = await readProjectEnv(project);
          const entries = await readManifest(project);
          const previousEffective = mergeShellEnvironment(previousGlobal, projectEnv);
          const nextEffective = mergeShellEnvironment(nextGlobal, projectEnv);
          const projectBashrc = await readProjectBashrc(project);
          if (podMode) {
            const info = await podRuntime.podInspect(project);
            if (!info?.running) return { count: 0, sessions: entries.length };
            await preparePodSessions(project, entries, entries, {
              podReady: true,
              shellConfig: {
                shellEnv: nextEffective,
                projectBashrc
              }
            });
          } else {
            for (const entry of entries) {
              if (!isCurrent()) return { count: 0, sessions: 0, stale: true };
              await ensureShellRecoveryFiles(project, entry.name, {
                shellEnv: nextEffective,
                projectBashrc
              });
            }
          }
          if (!isCurrent()) return { count: 0, sessions: 0, stale: true };
          return {
            count: await applyTmuxEnvironment(project, previousEffective, nextEffective),
            sessions: entries.length
          };
        });
        count += result.count;
        sessions += result.sessions;
      } catch {
        failures.push(project);
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(4, projects.length) }, () => worker()));
  return { count, sessions, projects: projects.length, failures: failures.sort(), stale: !isCurrent() };
}

async function getProjectBashrc(project) {
  assertProject(project);
  return serializeManifest(project, () => {
    assertProject(project);
    return readProjectBashrc(project);
  });
}

async function setProjectBashrc(project, content) {
  assertProject(project);
  const clean = validateProjectBashrc(content);
  return serializeManifest(project, async () => {
    assertProject(project);
    await atomicText(path.join(reaperRoot(project), "bashrc"), clean);
    const entries = await readManifest(project);
    if (podMode) {
      await preparePodSessions(project, entries, entries);
    } else {
      for (const entry of entries) await ensureShellRecoveryFiles(project, entry.name);
    }
    return { ok: true, sessions: entries.length };
  });
}

async function resetProjectState(project) {
  assertProject(project);
  return serializeManifest(project, async () => {
    assertProject(project);
    await fs.rm(reaperRoot(project), { recursive: true, force: true });
    await atomicJson(controlMarkerPath(project), { version: 1, initializedAt: nowIso() });
  });
}

async function listArchivedSessionLogs(project) {
  const directory = path.join(SESSION_ARCHIVE_DIR, "projects", String(project).replace(/[^A-Za-z0-9_.-]/g, "_"));
  let files = [];
  try { files = await fs.readdir(directory); } catch { return []; }
  const result = [];
  for (const file of files.sort().reverse()) {
    if (!file.endsWith(".log")) continue;
    const full = path.join(directory, file);
    const stat = await fs.stat(full);
    result.push({ file, sessionId: file.replace(/-[0-9]+\.log$/, ""), path: full, size: stat.size, mtime: stat.mtime.toISOString() });
  }
  return result;
}

function validatePorts(value) {
  if (!Array.isArray(value)) throw new Error("ports must be an array");
  if (value.length > MAX_PORTS_PER_PROJECT) throw new Error(`ports cannot exceed ${MAX_PORTS_PER_PROJECT} entries per project`);
  const seenPorts = new Set();
  const seenDomains = new Set();
  return value.map((item) => {
    const containerPort = Number(item?.containerPort);
    const subdomain = String(item?.subdomain || "").trim().toLowerCase();
    if (!Number.isInteger(containerPort) || containerPort < 1 || containerPort > 65535) throw new Error("containerPort must be an integer from 1 to 65535");
    if (!SUBDOMAIN_RE.test(subdomain)) throw new Error("subdomain must be a valid DNS label");
    if (seenPorts.has(containerPort) || seenDomains.has(subdomain)) throw new Error("ports and subdomains must be unique");
    seenPorts.add(containerPort); seenDomains.add(subdomain);
    return { containerPort, subdomain };
  }).sort((a, b) => a.subdomain.localeCompare(b.subdomain) || a.containerPort - b.containerPort);
}

const PUBLICATION_AUTH_FILE = "publication.json";

function validateRequireReaperAuth(value) {
  if (typeof value !== "boolean") throw new Error("requireReaperAuth must be a boolean");
  return value;
}

async function readRequireReaperAuth(project) {
  try {
    const value = JSON.parse(await fs.readFile(path.join(reaperRoot(project), PUBLICATION_AUTH_FILE), "utf8"));
    return validateRequireReaperAuth(value?.requireReaperAuth);
  } catch (error) {
    if (error.code === "ENOENT") return true;
    throw error;
  }
}

async function getProjectPorts(project) {
  assertProject(project);
  await ensureTrustedProjectState(project);
  let ports;
  try { ports = validatePorts(JSON.parse(await fs.readFile(path.join(reaperRoot(project), "ports.json"), "utf8"))); }
  catch (error) { if (error.code === "ENOENT") ports = []; else throw error; }
  return { ports, requireReaperAuth: await readRequireReaperAuth(project) };
}

function publicationConfig() {
  const configuredHost = String(process.env.REAPER_HOST || "").trim().toLowerCase();
  const configuredApex = String(process.env.APEX_DOMAIN || "").trim().toLowerCase().replace(/^\./, "");
  const host = configuredHost || configuredApex;
  if (DOMAIN_RE.test(host)) {
    if (configuredApex && configuredApex !== host) {
      throw new Error("REAPER_HOST and APEX_DOMAIN must match for domain-based published ports");
    }
    return { mode: "domain", host };
  }
  if (isIP(host)) {
    if (configuredApex) throw new Error("APEX_DOMAIN must be empty when REAPER_HOST is an IP address");
    return { mode: "ip", host, caddyHost: isIP(host) === 6 ? `[${host}]` : host };
  }
  throw new Error("REAPER_HOST or APEX_DOMAIN must be a valid DNS domain or IP address for published ports");
}

function assertPublicationPortAllowed(config, port) {
  if (config.mode !== "ip") return;
  if (port.containerPort < IP_PUBLISH_MIN_PORT || IP_PUBLISH_RESERVED_PORTS.has(port.containerPort)) {
    throw new Error(`containerPort must be an available host port from ${IP_PUBLISH_MIN_PORT} to 65535 for IP-based publishing`);
  }
}

function publicationKey(config, port) {
  return config.mode === "ip" ? port.containerPort : port.subdomain;
}

async function assertPublishedRoutesAvailable(project, ports) {
  if (!ports.length) return;
  const config = publicationConfig();
  for (const port of ports) assertPublicationPortAllowed(config, port);
  const requested = new Set(ports.map((port) => publicationKey(config, port)));
  const projects = (await fs.readdir(PROJECTS_ROOT, { withFileTypes: true }))
    .filter((entry) => entry.isDirectory() && !entry.name.startsWith(".") && entry.name !== project)
    .map((entry) => entry.name);
  for (const otherProject of projects) {
    const { ports: existing } = await getProjectPorts(otherProject);
    const conflict = existing.find((port) => requested.has(publicationKey(config, port)));
    if (!conflict) continue;
    if (config.mode === "ip") {
      throw new Error(`host port ${conflict.containerPort} is already published by project "${otherProject}"`);
    }
    throw new Error(`subdomain "${conflict.subdomain}" is already published by project "${otherProject}"`);
  }
}

function publishedCaddyBlock(config, port) {
  const address = config.mode === "ip"
    ? `https://${config.caddyHost}:${port.containerPort}`
    : `https://${port.subdomain}.${config.host}`;
  const tls = config.mode === "ip"
    ? "\n\ttls {\n\t\tissuer acme {\n\t\t\tdir https://acme-v02.api.letsencrypt.org/directory\n\t\t\tprofile shortlived\n\t\t}\n\t}"
    : "";
  const forwardAuth = port.requireReaperAuth !== false
    ? "\n\tforward_auth 127.0.0.1:4000 {\n\t\turi /api/auth/me\n\t}"
    : "";
  return `${address} {${tls}\n\theader {\n\t\t-Server\n\t\tX-Content-Type-Options "nosniff"\n\t\tX-Frame-Options "SAMEORIGIN"\n\t\tReferrer-Policy "same-origin"\n\t\tX-Robots-Tag "noindex, nofollow, noarchive"\n\t\tStrict-Transport-Security "max-age=31536000; includeSubDomains"\n\t}${forwardAuth}\n\treverse_proxy ${port.ip}:${port.containerPort} {\n\t\theader_up Cookie "(^|;[[:space:]]*)reaper_access=[^;]*" ""\n\t\theader_up Cookie "(^|;[[:space:]]*)reaper_csrf=[^;]*" ""\n\t\theader_down Set-Cookie "^reaper_(access|csrf)=.*$" ""\n\t}\n}`;
}

async function regenerateCaddyPorts({ quarantineInvalid = false, verifiedProjects = null } = {}) {
  const projects = (await fs.readdir(PROJECTS_ROOT, { withFileTypes: true }))
    .filter((entry) => entry.isDirectory() && !entry.name.startsWith("."))
    .map((entry) => entry.name)
    .sort();
  const publications = [];
  const quarantined = new Set();
  let config = null;
  for (const project of projects) {
    let ports;
    let requireReaperAuth;
    try {
      ({ ports, requireReaperAuth } = await getProjectPorts(project));
      if (!ports.length) continue;
      config ||= publicationConfig();
      for (const port of ports) assertPublicationPortAllowed(config, port);
      if (verifiedProjects && !verifiedProjects.has(project)) {
        quarantined.add(project);
        continue;
      }
      const info = await podRuntime.podInspect(project);
      if (!info?.running || !info?.ip || !info?.isolated) throw new Error("project pod is not isolated on its private network");
      for (const port of ports) publications.push({ project, ip: info.ip, requireReaperAuth, ...port });
    } catch (error) {
      if (!quarantineInvalid) throw error;
      quarantined.add(project);
      console.error(`[reaper] quarantined published ports for ${project}:`, error.message);
    }
  }
  const keys = new Set(publications.map((publication) => publicationKey(config, publication)));
  if (keys.size > MAX_GLOBAL_PUBLISHED_PORTS) {
    throw new Error(`published routes cannot exceed ${MAX_GLOBAL_PUBLISHED_PORTS} across the deployment`);
  }
  const ownersByRoute = new Map();
  for (const publication of publications) {
    const key = publicationKey(config, publication);
    if (!ownersByRoute.has(key)) ownersByRoute.set(key, new Set());
    ownersByRoute.get(key).add(publication.project);
  }
  for (const [key, owners] of ownersByRoute) {
    if (owners.size < 2) continue;
    for (const owner of owners) quarantined.add(owner);
    console.error(`[reaper] quarantined conflicting published ${config.mode === "ip" ? "host port" : "subdomain"} ${key}`);
  }
  const blocks = publications
    .filter((publication) => !quarantined.has(publication.project))
    .map((port) => publishedCaddyBlock(config, port));
  const target = process.env.CADDY_DYNAMIC_FILE || "/caddy-dynamic/ports.caddy";
  await fs.mkdir(path.dirname(target), { recursive: true });
  const temporary = `${target}.tmp-${process.pid}`;
  await fs.writeFile(temporary, `${blocks.join("\n\n")}${blocks.length ? "\n" : "# No published project ports.\n"}`, "utf8");
  await fs.rename(temporary, target);
  const entries = await fs.readdir(path.dirname(target), { withFileTypes: true });
  await Promise.all(entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".caddy") && entry.name !== path.basename(target))
    .map((entry) => fs.rm(path.join(path.dirname(target), entry.name), { force: true })));
  return { published: blocks.length, quarantined: [...quarantined].sort() };
}

async function updateProjectPortsUnlocked(project, clean, requireReaperAuth) {
  const operation = caddyPortOperation.then(async () => {
    const previous = await getProjectPorts(project);
    const nextRequireReaperAuth = requireReaperAuth === undefined
      ? previous.requireReaperAuth
      : validateRequireReaperAuth(requireReaperAuth);
    if (podMode) await assertPublishedRoutesAvailable(project, clean);
    try {
      await atomicJson(path.join(reaperRoot(project), "ports.json"), clean);
      await atomicJson(path.join(reaperRoot(project), PUBLICATION_AUTH_FILE), { requireReaperAuth: nextRequireReaperAuth });
      if (podMode) {
        await regenerateCaddyPorts();
        await podRuntime.reloadCaddy();
      }
      return { ports: clean, requireReaperAuth: nextRequireReaperAuth };
    } catch (error) {
      try {
        await atomicJson(path.join(reaperRoot(project), "ports.json"), previous.ports);
        await atomicJson(path.join(reaperRoot(project), PUBLICATION_AUTH_FILE), { requireReaperAuth: previous.requireReaperAuth });
        if (podMode) {
          await regenerateCaddyPorts();
          await podRuntime.reloadCaddy();
        }
      } catch (rollbackError) {
        throw new AggregateError([error, rollbackError], "published-port update failed and rollback could not restore Caddy");
      }
      throw error;
    }
  });
  caddyPortOperation = operation.catch(() => {});
  return operation;
}

async function updateProjectPorts(project, ports, requireReaperAuth) {
  assertProject(project);
  const clean = validatePorts(ports);
  if (requireReaperAuth !== undefined) validateRequireReaperAuth(requireReaperAuth);
  if (!podMode && clean.length) throw new Error("published ports require the pod runtime");
  return serializeManifest(project, () => {
    assertProject(project);
    return updateProjectPortsUnlocked(project, clean, requireReaperAuth);
  });
}

async function destroyProjectRuntime(project) {
  assertProject(project);
  return serializeManifest(project, async () => {
    assertProject(project);
    let routeCleanupPending = false;
    const routeOperation = caddyPortOperation.then(async () => {
      await atomicJson(path.join(reaperRoot(project), "ports.json"), []);
      if (!podMode) return;
      await regenerateCaddyPorts();
      await podRuntime.reloadCaddy();
    });
    caddyPortOperation = routeOperation.catch(() => {});
    try {
      await routeOperation;
    } catch (error) {
      routeCleanupPending = true;
      console.error(`[reaper] published routes could not be reloaded before deleting ${project}:`, error.message);
    }

    const entries = await readManifest(project, { createDefault: false });
    const archivedLogs = [];
    if (podMode) {
      for (const entry of entries) {
        try {
          const archivedLog = await archiveSessionLog(project, entry.name, { remove: false });
          if (archivedLog) archivedLogs.push(archivedLog);
        } catch (error) {
          console.error(`[reaper] failed to archive project-deletion session ${project}/${entry.name}:`, error.message);
        }
      }
      await podRuntime.destroyPod(project);
      for (const entry of entries) {
        detachSessionViewers(project, entry.name, "SESSION_DELETED");
        broadcastSessionEvent("deleted", project, entry);
      }
    } else {
      for (const entry of entries) {
        const result = await destroySessionUnlocked(project, entry.name);
        if (result.archivedLog) archivedLogs.push(result.archivedLog);
      }
    }
    await fs.rm(projectRoot(project), { recursive: true, force: true });
    await fs.rm(reaperRoot(project), { recursive: true, force: true });
    return { ok: true, count: entries.length, archivedLogs, routeCleanupPending };
  });
}

function nextSequence(stream) { stream.outSequence += 1; return stream.outSequence; }
function sendFrame(connection, type, streamId, payload = EMPTY_BUFFER, flags = 0, sequence) {
  if (connection.ws.readyState !== 1) return false;
  const seq = sequence ?? (streamId ? nextSequence(connection.streams.get(streamId)) : ++connection.controlSequence);
  const encoded = encodeFrame({ type, flags, streamId, sequence: seq, payload });
  if (connection.ws.bufferedAmount + encoded.byteLength > MAX_BUFFERED_AMOUNT) {
    connection.ws.close(1009, "terminal control backpressure");
    return false;
  }
  connection.ws.send(encoded, { binary: true });
  return seq;
}
function sendJson(connection, type, streamId, value, flags = 0) { return sendFrame(connection, type, streamId, encodeJson(value), flags); }

function broadcastSessionEvent(event, project, session) {
  const presented = presentSession(project, session, event === "deleted" ? "deleted" : "running");
  const payload = {
    event,
    project,
    session: event === "activity"
      ? {
          name: presented.name,
          state: presented.state,
          attachedClients: presented.attachedClients,
          ...(presented.lastInteractionAt ? { lastInteractionAt: presented.lastInteractionAt } : {})
        }
      : presented
  };
  for (const connection of connections) if (connection.hello) sendJson(connection, TYPES.SESSION_EVENT, 0, payload);
}

function streamViewerSet(project, name) {
  const id = sessionId(project, name);
  if (!viewersBySession.has(id)) viewersBySession.set(id, new Set());
  return viewersBySession.get(id);
}

function reserveStream(connection) {
  const userStreams = activeStreamsByUser.get(connection.userId) || 0;
  if (activeStreams >= MAX_ACTIVE_STREAMS || userStreams >= MAX_ACTIVE_STREAMS_PER_USER) return null;
  activeStreams += 1;
  activeStreamsByUser.set(connection.userId, userStreams + 1);
  return { userId: connection.userId, claimed: false, released: false };
}

function releaseStreamReservation(reservation) {
  if (!reservation || reservation.released) return;
  reservation.released = true;
  activeStreams = Math.max(0, activeStreams - 1);
  const count = Math.max(0, (activeStreamsByUser.get(reservation.userId) || 0) - 1);
  if (count) activeStreamsByUser.set(reservation.userId, count);
  else activeStreamsByUser.delete(reservation.userId);
}

function releaseConnection(connection) {
  if (connection.released) return;
  connection.released = true;
  connections.delete(connection);
  const count = Math.max(0, (connectionsByUser.get(connection.userId) || 0) - 1);
  if (count) connectionsByUser.set(connection.userId, count);
  else connectionsByUser.delete(connection.userId);
}

function cleanupViewerProcess(stream) {
  if (!stream.viewerMarker) return null;
  if (stream.cleanupPromise) return stream.cleanupPromise;
  const cleanup = terminateMarkedProcesses(
    stream.project,
    stream.viewerMarker,
    "exact",
    `terminal viewer for ${stream.project}/${stream.name}`
  ).catch((error) => {
    console.error(`[reaper] failed to clean terminal viewer ${stream.project}/${stream.name}:`, error.message);
  });
  stream.cleanupPromise = cleanup;
  viewerCleanupOperations.add(cleanup);
  cleanup.finally(() => viewerCleanupOperations.delete(cleanup));
  return cleanup;
}

function detachStream(connection, streamId, code = null, notify = false) {
  const stream = connection.streams.get(streamId);
  if (!stream) return;
  clearTimeout(stream.outputTimer);
  clearTimeout(stream.slowTimer);
  clearTimeout(stream.resizeTimer);
  stream.closed = true;
  stream.cancelControlInput?.(new Error("terminal stream closed"));
  clearInterval(stream.pressurePoll);
  for (const waiter of stream.capacityWaiters.splice(0)) waiter.resolve(false);
  let releaseOperation = null;
  if (podMode) {
    try { stream.pty?.kill(); } catch {}
    const cleanup = cleanupViewerProcess(stream);
    releaseOperation = Promise.allSettled([stream.inputOperation, cleanup]);
  } else stream.fallback?.detach(stream.viewer);
  const set = streamViewerSet(stream.project, stream.name);
  set.delete(stream);
  if (stream.entry) broadcastSessionEvent("activity", stream.project, stream.entry);
  if (set.size) {
    const remaining = set.values().next().value;
    resizeSession(remaining, remaining.cols, remaining.rows, { force: true });
  } else viewersBySession.delete(sessionId(stream.project, stream.name));
  if (notify) {
    if (code) sendJson(connection, TYPES.CLOSE_STREAM, streamId, { code }, FLAGS.ERROR);
    else sendFrame(connection, TYPES.CLOSE_STREAM, streamId);
  }
  if (releaseOperation) releaseOperation.then(() => releaseStreamReservation(stream.reservation));
  else releaseStreamReservation(stream.reservation);
  connection.streams.delete(streamId);
}

function detachSessionViewers(project, name, code) {
  for (const stream of [...streamViewerSet(project, name)]) detachStream(stream.connection, stream.id, code, true);
}

function sessionViewport(viewers) {
  let cols = MAX_TERMINAL_COLS;
  let rows = MAX_TERMINAL_ROWS;
  let found = false;
  for (const viewer of viewers) {
    if (viewer.closed) continue;
    found = true;
    cols = Math.min(cols, viewer.cols);
    rows = Math.min(rows, viewer.rows);
  }
  return found ? { cols, rows } : { cols: 1, rows: 1 };
}

function notifySessionViewport(viewer, viewport) {
  if (!viewer.ready || viewer.closed) return;
  sendJson(viewer.connection, TYPES.STATUS, viewer.id, {
    state: "viewport",
    cols: viewport.cols,
    rows: viewport.rows
  });
}

function applySessionViewport(viewers, viewport) {
  for (const viewer of viewers) {
    try { viewer.pty?.resize(viewport.cols, viewport.rows); } catch {}
    viewer.fallback?.resize(viewport.cols, viewport.rows);
  }
  for (const viewer of viewers) notifySessionViewport(viewer, viewport);
}

function resizeSession(stream, cols, rows, { force = false } = {}) {
  cols = Math.max(1, Math.min(MAX_TERMINAL_COLS, Number(cols) || 1));
  rows = Math.max(1, Math.min(MAX_TERMINAL_ROWS, Number(rows) || 1));
  const viewers = streamViewerSet(stream.project, stream.name);
  const previous = sessionViewport(viewers);
  stream.cols = cols;
  stream.rows = rows;
  const viewport = sessionViewport(viewers);
  if (!force && previous.cols === viewport.cols && previous.rows === viewport.rows) {
    notifySessionViewport(stream, viewport);
    return false;
  }
  applySessionViewport(viewers, viewport);
  return true;
}

function scheduleResizeSession(stream, cols, rows) {
  stream.pendingResize = { cols, rows };
  if (stream.resizeTimer) return;
  stream.resizeTimer = setTimeout(() => {
    stream.resizeTimer = null;
    const pending = stream.pendingResize;
    stream.pendingResize = null;
    if (!stream.closed && pending) resizeSession(stream, pending.cols, pending.rows);
  }, RESIZE_COALESCE_MS);
}

function hasStreamCapacity(stream, payloadLength = 0) {
  return !stream.closed &&
    stream.unackedBytes + payloadLength <= MAX_UNACKED_BYTES &&
    stream.connection.ws.bufferedAmount + HEADER_SIZE + payloadLength <= MAX_BUFFERED_AMOUNT;
}

function updateBackpressure(stream, requestedBytes = 0) {
  if (stream.closed) return true;
  const requiredBytes = Math.max(
    requestedBytes,
    ...stream.capacityWaiters.map((waiter) => waiter.requiredBytes)
  );
  const overloaded = !hasStreamCapacity(stream, requiredBytes);
  if (overloaded) {
    if (!stream.paused && stream.pty?.pause) {
      stream.pty.pause();
      stream.paused = true;
    }
    if (!stream.slowTimer) {
      stream.slowTimer = setTimeout(
        () => detachStream(stream.connection, stream.id, "SLOW_CLIENT", true),
        BACKPRESSURE_TIMEOUT_MS
      );
    }
    if (!stream.pressurePoll) {
      stream.pressurePoll = setInterval(() => updateBackpressure(stream), 25);
    }
    return true;
  }
  clearTimeout(stream.slowTimer);
  clearInterval(stream.pressurePoll);
  stream.slowTimer = null;
  stream.pressurePoll = null;
  if (stream.paused && !stream.replayingHistory && stream.pty?.resume) {
    stream.pty.resume();
    stream.paused = false;
  }
  const ready = stream.capacityWaiters.splice(0);
  for (const waiter of ready) waiter.resolve(true);
  return false;
}

function waitForStreamCapacity(stream, requiredBytes = 0) {
  if (hasStreamCapacity(stream, requiredBytes)) return Promise.resolve(true);
  return new Promise((resolve) => {
    stream.capacityWaiters.push({ requiredBytes, resolve });
    updateBackpressure(stream, requiredBytes);
  });
}

function accountAndSend(stream, type, payload, flags = 0) {
  if (!hasStreamCapacity(stream, payload.length)) {
    updateBackpressure(stream, payload.length);
    return false;
  }
  const sequence = sendFrame(stream.connection, type, stream.id, payload, flags);
  if (!sequence) return false;
  stream.sentPayload.set(sequence, payload.length);
  stream.unackedBytes += payload.length;
  updateBackpressure(stream);
  return true;
}

function flushOutput(stream) {
  stream.outputTimer = null;
  if (!stream.outputBuffers.length || stream.closed || stream.outputBlocked) return;
  const length = Math.min(stream.outputBytes, OUTPUT_MAX_PAYLOAD);
  let payload = stream.pendingOutputPayload;
  if (!payload) {
    if (stream.outputBuffers[0].length >= length) payload = stream.outputBuffers[0].subarray(0, length);
    else {
      const parts = [];
      let remaining = length;
      for (const buffer of stream.outputBuffers) {
        if (!remaining) break;
        const part = buffer.subarray(0, remaining);
        parts.push(part);
        remaining -= part.length;
      }
      payload = Buffer.concat(parts, length);
    }
  }
  if (!accountAndSend(stream, TYPES.OUTPUT, payload)) {
    stream.pendingOutputPayload = payload;
    stream.outputBlocked = true;
    void waitForStreamCapacity(stream, payload.length).then((available) => {
      stream.outputBlocked = false;
      if (available) flushOutput(stream);
    });
    return;
  }
  stream.pendingOutputPayload = null;
  let remaining = length;
  while (remaining > 0) {
    const first = stream.outputBuffers[0];
    if (first.length <= remaining) {
      remaining -= first.length;
      stream.outputBuffers.shift();
    } else {
      stream.outputBuffers[0] = first.subarray(remaining);
      remaining = 0;
    }
  }
  stream.outputBytes -= length;
  if (stream.outputBytes && !stream.outputTimer) {
    stream.outputTimer = setTimeout(() => flushOutput(stream), stream.outputBytes >= OUTPUT_MAX_PAYLOAD ? 0 : 4);
  }
}

function queueOutput(stream, data) {
  const buffer = Buffer.isBuffer(data) ? data : Buffer.from(data);
  if (!buffer.length || stream.closed) return;
  if (!stream.ready) {
    stream.preReady.push(buffer);
    return;
  }
  stream.outputBuffers.push(buffer);
  stream.outputBytes += buffer.length;
  if (stream.outputBytes >= OUTPUT_MAX_PAYLOAD) flushOutput(stream);
  if (stream.outputBytes && !stream.outputTimer && !stream.outputBlocked) {
    stream.outputTimer = setTimeout(() => flushOutput(stream), 4);
  }
}

function settlePodInputBatch(stream, batch, error = null) {
  for (const entry of batch) {
    stream.pendingInputBytes -= entry.bytes;
    stream.pendingInputFrames -= 1;
    if (error) entry.reject(error);
    else entry.resolve();
  }
}

async function runPodInputPump(stream) {
  while (stream.inputQueue.length) {
    if (stream.closed) {
      const error = new Error("terminal stream closed");
      settlePodInputBatch(stream, stream.inputQueue.splice(0), error);
      break;
    }

    const batch = [];
    let batchBytes = 0;
    while (stream.inputQueue.length) {
      const next = stream.inputQueue[0];
      if (batch.length && batchBytes + next.bytes > INPUT_BATCH_MAX_BYTES) break;
      batch.push(stream.inputQueue.shift());
      batchBytes += next.bytes;
    }

    try {
      const input = batch.map((entry) => entry.input).join("");
      if (stream.controlInput && batchBytes <= CONTROL_INPUT_FAST_PATH_BYTES) {
        await stream.controlInput(input);
      } else {
        const result = await podRuntime.podExec(stream.project, [
          "tmux", "-S", TMUX_SOCKET, "send-keys", "-t", `=${stream.name}:`, "-l", "--", input
        ]);
        if (result.code !== 0) throw new Error(result.stderr || "terminal input failed");
      }
      settlePodInputBatch(stream, batch);
    } catch (error) {
      stream.inputFailure = error;
      settlePodInputBatch(stream, batch, error);
      settlePodInputBatch(stream, stream.inputQueue.splice(0), error);
      break;
    }
  }
  stream.inputPumpRunning = false;
}

async function sendPodInput(stream, input) {
  const bytes = Buffer.byteLength(input);
  if (!bytes) return;
  if (stream.inputFailure) throw stream.inputFailure;
  if (
    stream.pendingInputBytes + bytes > MAX_PENDING_INPUT_BYTES ||
    stream.pendingInputFrames >= MAX_PENDING_INPUT_FRAMES
  ) throw new Error("terminal input queue limit exceeded");
  stream.pendingInputBytes += bytes;
  stream.pendingInputFrames += 1;

  const completion = new Promise((resolve, reject) => {
    stream.inputQueue.push({ input, bytes, resolve, reject });
  });
  if (!stream.inputPumpRunning) {
    stream.inputPumpRunning = true;
    stream.inputOperation = runPodInputPump(stream);
  }
  return completion;
}

function decodeTmuxControlData(value) {
  const source = Buffer.isBuffer(value) ? value : Buffer.from(value);
  const output = Buffer.allocUnsafe(source.length);
  let read = 0;
  let written = 0;
  while (read < source.length) {
    if (source[read] !== 0x5c) {
      output[written++] = source[read++];
      continue;
    }
    if (source[read + 1] === 0x5c) {
      output[written++] = 0x5c;
      read += 2;
      continue;
    }
    if (
      read + 3 >= source.length ||
      source[read + 1] < 0x30 || source[read + 1] > 0x37 ||
      source[read + 2] < 0x30 || source[read + 2] > 0x37 ||
      source[read + 3] < 0x30 || source[read + 3] > 0x37
    ) throw new Error("malformed tmux control escape");
    output[written++] =
      ((source[read + 1] - 0x30) << 6) |
      ((source[read + 2] - 0x30) << 3) |
      (source[read + 3] - 0x30);
    read += 4;
  }
  return output.subarray(0, written);
}

function terminalModeBaseline(value) {
  const flags = value.toString("ascii").split(",");
  if (flags.length !== 10 || flags.some((flag) => flag !== "0" && flag !== "1")) {
    throw new Error("malformed tmux pane mode response");
  }
  const enabled = (index, mode) => `\u001b[?${mode}${flags[index] === "1" ? "h" : "l"}`;
  let mouseMode = "1000";
  if (flags[6] === "1") mouseMode = "1003";
  else if (flags[5] === "1") mouseMode = "1002";
  return Buffer.from(
    enabled(0, "1049") +
    enabled(1, "25") +
    enabled(2, "1") +
    (flags[3] === "1" ? "\u001b=" : "\u001b>") +
    "\u001b[?1000l\u001b[?1002l\u001b[?1003l\u001b[?1005l\u001b[?1006l" +
    ((flags[4] === "1" || flags[5] === "1" || flags[6] === "1") ? `\u001b[?${mouseMode}h` : "") +
    (flags[7] === "1" ? "\u001b[?1005h" : "") +
    (flags[8] === "1" ? "\u001b[?1006h" : "") +
    enabled(9, "2004"),
    "ascii"
  );
}

function startControlHandoff(stream, viewport = { cols: stream.cols, rows: stream.rows }) {
  const rawPty = podRuntime.podExecPty(stream.project, [
    "env", stream.viewerMarker, "sh", "-c", CAPTURE_POD_SESSION_SCRIPT,
    "reaper-capture-session",
    TMUX_SOCKET,
    TMUX_CONFIG,
    stream.name,
    podLogPath(stream.name),
    `/work/.reaper/logs/${stream.name}.log`,
    `cat >> ${shellQuote(podLogPath(stream.name))}`
  ], { cols: viewport.cols, rows: viewport.rows });
  stream.pty = {
    pause: rawPty.pause ? () => rawPty.pause() : undefined,
    resume: rawPty.resume ? () => rawPty.resume() : undefined,
    resize: (cols, rows) => {
      rawPty.resize?.(cols, rows);
      queueControlResize(cols, rows);
    },
    kill: rawPty.kill ? () => rawPty.kill() : undefined
  };
  let settled = false;
  let failed = false;
  let streamIngressWindowAt = Date.now();
  let streamIngressBytes = 0;
  let handoffPhase = "attach";
  let captureBlock = null;
  let resizeCommandPending = null;
  let liveCommandActive = null;
  const liveInputCommands = [];
  let mode = null;
  let historyRows = [];
  let historyStart = 0;
  let historyBytes = 0;
  let truncated = false;
  let lineParts = [];
  let lineBytes = 0;
  let droppingLine = false;
  let diagnostics = "";
  let resolveBoundary;
  let rejectBoundary;
  const boundary = new Promise((resolve, reject) => {
    resolveBoundary = resolve;
    rejectBoundary = reject;
  });
  const fail = (error) => {
    if (failed) return;
    failed = true;
    if (settled) {
      if (!stream.closed) detachStream(stream.connection, stream.id, "CONTROL_PROTOCOL_ERROR", true);
      return;
    }
    settled = true;
    rejectBoundary(error instanceof Error ? error : new Error(String(error)));
  };
  const handoffTimer = setTimeout(
    () => fail(new Error("tmux control handoff timed out")),
    CONTROL_HANDOFF_TIMEOUT_MS
  );
  const writeControlResize = (cols, rows) => {
    rawPty.write(`refresh-client -C ${cols},${rows}\n`);
  };
  const rejectLiveCommands = (error) => {
    const failure = error instanceof Error ? error : new Error(String(error));
    const pending = liveCommandActive
      ? [liveCommandActive, ...liveInputCommands.splice(0)]
      : liveInputCommands.splice(0);
    liveCommandActive = null;
    for (const command of pending) command.reject?.(failure);
  };
  const flushControlCommand = () => {
    if (
      failed ||
      stream.closed ||
      handoffPhase !== "live" ||
      captureBlock ||
      liveCommandActive
    ) return;
    let next = liveInputCommands.shift();
    if (!next && resizeCommandPending) {
      const resize = resizeCommandPending;
      resizeCommandPending = null;
      next = {
        kind: "resize",
        command: `refresh-client -C ${resize.cols},${resize.rows}\n`
      };
    }
    if (!next) return;
    liveCommandActive = next;
    try {
      rawPty.write(next.command);
    } catch (error) {
      liveCommandActive = null;
      next.reject?.(error);
      fail(error);
    }
  };
  const issueInitialResize = () => {
    handoffPhase = "resize";
    try {
      writeControlResize(viewport.cols, viewport.rows);
    } catch (error) {
      fail(error);
    }
  };
  const queueControlResize = (cols, rows) => {
    resizeCommandPending = { cols, rows };
    flushControlCommand();
  };
  const queueControlInput = (input) => {
    const bytes = Buffer.from(input, "utf8");
    if (!bytes.length) return Promise.resolve();
    const keys = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(" ");
    return new Promise((resolve, reject) => {
      liveInputCommands.push({
        kind: "input",
        command: `send-keys -t "=${stream.name}:" -H ${keys}\n`,
        resolve,
        reject
      });
      flushControlCommand();
    });
  };
  stream.controlInput = queueControlInput;
  stream.cancelControlInput = rejectLiveCommands;
  const issueMode = () => {
    handoffPhase = "mode";
    try {
      rawPty.write(`display-message -p -t "=${stream.name}:" "${CONTROL_MODE_FORMAT}"\n`);
    } catch (error) {
      fail(error);
    }
  };
  const issueCapture = () => {
    handoffPhase = "history";
    try {
      rawPty.write(`capture-pane -p -e -J -t "=${stream.name}:" -S -\n`);
    } catch (error) {
      fail(error);
    }
  };
  const retainRow = (row) => {
    const clean = Buffer.from(stripTerminalQueries(row.toString("utf8")).replace(/\r?\n/g, ""), "utf8");
    const rendered = Buffer.concat([clean, Buffer.from("\r\n")]);
    if (rendered.length > CONTROL_HISTORY_LIMIT) {
      historyRows = [];
      historyStart = 0;
      historyBytes = 0;
      truncated = true;
      return;
    }
    while (
      historyBytes + rendered.length > CONTROL_HISTORY_LIMIT ||
      historyRows.length - historyStart >= CONTROL_HISTORY_ROW_LIMIT
    ) {
      historyBytes -= historyRows[historyStart].length;
      historyRows[historyStart++] = null;
      truncated = true;
    }
    if (historyStart >= 4096 && historyStart * 2 >= historyRows.length) {
      historyRows = historyRows.slice(historyStart);
      historyStart = 0;
    }
    historyRows.push(rendered);
    historyBytes += rendered.length;
  };
  const finishCapture = () => {
    if (!mode) return fail(new Error("tmux capture omitted pane mode response"));
    let prefix;
    try {
      prefix = Buffer.concat([
        truncated ? Buffer.from("\u001bc", "ascii") : EMPTY_BUFFER,
        terminalModeBaseline(mode)
      ]);
    } catch (error) {
      return fail(error);
    }
    if (failed) return;
    handoffPhase = "live";
    settled = true;
    const retainedRows = historyRows.slice(historyStart);
    const retainedBytes = retainedRows.length ? historyBytes - 2 : historyBytes;
    if (retainedRows.length) retainedRows[retainedRows.length - 1] = retainedRows.at(-1).subarray(0, -2);
    resolveBoundary(Buffer.concat([prefix, ...retainedRows], prefix.length + retainedBytes));
    flushControlCommand();
  };
  const handleLine = (line) => {
    if (line.subarray(0, CONTROL_DCS_PREFIX.length).equals(CONTROL_DCS_PREFIX)) {
      line = line.subarray(CONTROL_DCS_PREFIX.length);
    }
    if (!line.length || line.equals(CONTROL_DCS_SUFFIX)) return;
    if (line.length && line.at(-1) === 0x0d) line = line.subarray(0, -1);
    const text = line.toString("ascii");
    if (text === "REAPER_SESSION_MISSING") {
      diagnostics = text;
      return;
    }
    if (text.startsWith("%output ")) {
      const paneEnd = line.indexOf(0x20, 8);
      if (paneEnd < 0) return fail(new Error("malformed tmux %output"));
      let output;
      try { output = decodeTmuxControlData(line.subarray(paneEnd + 1)); }
      catch (error) { return fail(error); }
      if (settled) queueOutput(stream, output);
      return;
    }
    if (text.startsWith("%begin ")) {
      if (captureBlock) return fail(new Error("nested tmux control response"));
      captureBlock = text.slice(7);
      return;
    }
    if (text.startsWith("%end ") || text.startsWith("%error ")) {
      const isError = text.startsWith("%error ");
      const signature = text.slice(isError ? 7 : 5);
      if (!captureBlock || signature !== captureBlock) return fail(new Error("unmatched tmux control boundary"));
      captureBlock = null;
      const completedPhase = handoffPhase;
      if (completedPhase === "live") {
        const command = liveCommandActive;
        liveCommandActive = null;
        if (!command) return fail(new Error("unexpected tmux live command response"));
        if (isError) {
          const error = new Error(`tmux ${command.kind} command failed`);
          command.reject?.(error);
          return fail(error);
        }
        command.resolve?.();
        flushControlCommand();
        return;
      }
      if (isError) {
        const detail = completedPhase === "history"
          ? historyRows.at(-1)?.toString("utf8").trim()
          : mode?.toString("utf8").trim();
        return fail(new Error(detail || diagnostics || `tmux ${completedPhase} command failed`));
      }
      if (completedPhase === "attach") issueInitialResize();
      else if (completedPhase === "resize") issueMode();
      else if (completedPhase === "mode") issueCapture();
      else if (completedPhase === "history") finishCapture();
      return;
    }
    if (captureBlock && (handoffPhase === "mode" || handoffPhase === "history")) {
      if (handoffPhase === "mode") {
        if (mode) return fail(new Error("tmux pane mode response contained multiple lines"));
        mode = Buffer.from(line);
      } else {
        retainRow(line);
      }
      return;
    }
    if (text.startsWith("%")) return;
    if (text) diagnostics = `${diagnostics}${diagnostics ? "\n" : ""}${text}`.slice(-4096);
  };
  const feed = (data) => {
    if (failed || stream.closed) return;
    const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);
    const now = Date.now();
    if (now - streamIngressWindowAt >= 1000) {
      streamIngressWindowAt = now;
      streamIngressBytes = 0;
    }
    if (now - controlIngressWindowAt >= 1000) {
      controlIngressWindowAt = now;
      controlIngressBytes = 0;
    }
    streamIngressBytes += chunk.length;
    controlIngressBytes += chunk.length;
    if (
      streamIngressBytes > CONTROL_INGRESS_BYTES_PER_SECOND ||
      controlIngressBytes > CONTROL_GLOBAL_INGRESS_BYTES_PER_SECOND
    ) {
      try { rawPty.pause?.(); } catch {}
      fail(new Error("tmux control ingress rate exceeded"));
      return;
    }
    let start = 0;
    for (let index = 0; index < chunk.length; index += 1) {
      if (chunk[index] !== 0x0a) continue;
      const part = chunk.subarray(start, index);
      if (!droppingLine && lineBytes + part.length <= CONTROL_LINE_LIMIT) {
        lineParts.push(part);
        lineBytes += part.length;
        const line = lineParts.length === 1 ? lineParts[0] : Buffer.concat(lineParts, lineBytes);
        handleLine(line);
        if (failed) return;
      } else {
        truncated = true;
      }
      lineParts = [];
      lineBytes = 0;
      droppingLine = false;
      start = index + 1;
    }
    const tail = chunk.subarray(start);
    if (!droppingLine && lineBytes + tail.length <= CONTROL_LINE_LIMIT) {
      lineParts.push(tail);
      lineBytes += tail.length;
    } else {
      lineParts = [];
      lineBytes = 0;
      droppingLine = true;
      truncated = true;
    }
  };
  rawPty.onData(feed);
  rawPty.onExit?.(({ exitCode } = {}) => {
    if (!settled) {
      const missing = exitCode === 42 || /REAPER_SESSION_MISSING|can't find session/i.test(diagnostics);
      const error = new Error(missing ? "REAPER_SESSION_MISSING" : diagnostics || `tmux control client exited (${exitCode ?? 0})`);
      error.sessionMissing = missing;
      fail(error);
    } else if (!stream.closed && !stream.restartingControl) {
      detachStream(stream.connection, stream.id, `VIEWER_EXIT_${exitCode ?? 0}`, true);
    }
  });
  return boundary.finally(() => clearTimeout(handoffTimer));
}

async function openStream(connection, request, reservation) {
  const project = assertProject(String(request?.project || ""));
  const name = normalizeSessionName(request?.sessionName);
  const cols = Math.max(1, Math.min(MAX_TERMINAL_COLS, Number(request?.cols) || 120));
  const rows = Math.max(1, Math.min(MAX_TERMINAL_ROWS, Number(request?.rows) || 32));
  let result = null;
  let entry;
  let history = EMPTY_BUFFER;
  if (podMode) {
    entry = await readExistingSessionEntry(project, name);
    const inspected = await podRuntime.podInspect(project);
    if (!inspected.running) {
      result = await openProjectShell({ path: project, sessionName: name, cols, rows, createIfMissing: false });
      entry = result.session;
    }
    if (connection.ws.readyState !== 1) return;
  } else {
    result = await openProjectShell({ path: project, sessionName: name, cols, rows, createIfMissing: false });
    entry = result.session;
    if (connection.ws.readyState !== 1) return;
  }
  let id = connection.nextStreamId++;
  if (id === 0) id = connection.nextStreamId++;
  reservation.claimed = true;
  const stream = { id, connection, project, name, entry: Object.freeze({ ...entry }), cols, rows, reservation, outSequence: 0, inSequence: 0, lastAckSequence: 0, inputDecoder: new StringDecoder("utf8"), inputOperation: Promise.resolve(), pendingInputBytes: 0, pendingInputFrames: 0, sentPayload: new Map(), unackedBytes: 0, preReady: [], ready: false, replayingHistory: true, paused: false, closed: false, outputBuffers: [], outputBytes: 0, pendingOutputPayload: null, outputBlocked: false, outputTimer: null, slowTimer: null, pressurePoll: null, capacityWaiters: [], lastActivityAt: 0, viewerMarker: null, cleanupPromise: null };
  stream.inputQueue = [];
  stream.inputPumpRunning = false;
  stream.inputFailure = null;
  stream.pendingResize = null;
  stream.resizeTimer = null;
  connection.streams.set(id, stream);
  streamViewerSet(project, name).add(stream);
  let viewport = sessionViewport(streamViewerSet(project, name));
  if (podMode) {
    stream.viewerMarker = `REAPER_VIEWER_ID=${randomUUID()}`;
    let repaired = false;
    try {
      while (true) {
        try {
          viewport = sessionViewport(streamViewerSet(project, name));
          applySessionViewport(streamViewerSet(project, name), viewport);
          history = await startControlHandoff(stream, viewport);
          break;
        } catch (error) {
          stream.restartingControl = true;
          try { stream.pty?.kill(); } catch {}
          stream.restartingControl = false;
          if (!error.sessionMissing || repaired || connection.ws.readyState !== 1) throw error;
          repaired = true;
          result = await openProjectShell({ path: project, sessionName: name, cols, rows, createIfMissing: false });
          entry = result.session;
          stream.entry = Object.freeze({ ...entry });
        }
      }
      if (connection.ws.readyState !== 1 || stream.closed) {
        detachStream(connection, id);
        return;
      }
    } catch (error) {
      detachStream(connection, id);
      throw error;
    }
  } else {
    stream.fallback = result.runtime;
    stream.viewer = { onData: (data) => queueOutput(stream, data), onExit: (code) => { if (!stream.closed) sendJson(connection, TYPES.STATUS, id, { state: "closed", code, degraded: true }); } };
    stream.fallback.attach(stream.viewer);
  }
  broadcastSessionEvent("activity", project, stream.entry);
  const completedViewport = sessionViewport(streamViewerSet(project, name));
  if (completedViewport.cols !== viewport.cols || completedViewport.rows !== viewport.rows) {
    applySessionViewport(streamViewerSet(project, name), completedViewport);
  }
  viewport = completedViewport;
  sendJson(connection, TYPES.OPENED, id, {
    requestId: request.requestId,
    project,
    sessionName: name,
    title: stream.entry.title,
    degraded: !podMode,
    cols: viewport.cols,
    rows: viewport.rows
  });
  if (!history.length) {
    while (!accountAndSend(stream, TYPES.HISTORY, EMPTY_BUFFER, FLAGS.FINAL)) {
      if (!await waitForStreamCapacity(stream, 0)) return;
    }
  } else {
    for (let offset = 0; offset < history.length && !stream.closed;) {
      const chunk = history.subarray(offset, offset + HISTORY_CHUNK_SIZE);
      const flags = offset + chunk.length >= history.length ? FLAGS.FINAL : 0;
      if (!accountAndSend(stream, TYPES.HISTORY, chunk, flags)) {
        if (!await waitForStreamCapacity(stream, chunk.length)) return;
        continue;
      }
      offset += chunk.length;
    }
  }
  if (stream.closed) return;
  sendJson(connection, TYPES.READY, id, { cols: viewport.cols, rows: viewport.rows });
  stream.ready = true;
  stream.replayingHistory = false;
  updateBackpressure(stream);
  for (const buffered of stream.preReady) queueOutput(stream, buffered);
  stream.preReady = [];
}

function terminateTerminalConnection(connection, code, reason) {
  if (connection.revoked) return;
  connection.revoked = true;
  if (connection.ws.readyState === 1) connection.ws.close(code, reason);
  connection.ws.terminate?.();
}

function protocolError(connection, code, message, streamId = 0) {
  try { sendJson(connection, TYPES.PROTOCOL_ERROR, streamId, { code, message }, FLAGS.ERROR); } catch {}
  terminateTerminalConnection(connection, 1002, String(code).slice(0, 123));
}

function closeTerminalConnectionsForSession(authSessionId, code = 4003, reason = "authentication revoked") {
  for (const connection of connections) {
    if (connection.request?.reaperAuth?.jti === authSessionId) terminateTerminalConnection(connection, code, reason);
  }
}

function attachTerminalWebSocket(wss, { verifyCsrf } = {}) {
  wss.on("connection", (ws, request) => {
    request.socket.setNoDelay(true);
    const userId = String(request.reaperAuth?.sub || "anonymous");
    const userConnections = connectionsByUser.get(userId) || 0;
    if (connections.size >= MAX_CONNECTIONS || userConnections >= MAX_CONNECTIONS_PER_USER) {
      ws.close(1008, "terminal connection limit exceeded");
      ws.terminate?.();
      return;
    }
    const connection = { ws, request, userId, released: false, revoked: false, hello: false, streams: new Map(), pendingOpens: 0, nextStreamId: 1, controlSequence: 0, lastPongAt: Date.now(), lastServerPingSequence: 0, lastClientPingAt: 0, missedPongs: 0, authExpiry: null };
    connection.ingressWindowAt = Date.now();
    connection.ingressFrames = 0;
    connection.ingressBytes = 0;
    const authExpiresAt = Number(request.reaperAuth?.exp) * 1000;
    if (Number.isFinite(authExpiresAt)) {
      connection.authExpiry = setTimeout(
        () => terminateTerminalConnection(connection, 4003, "authentication expired"),
        Math.max(1, authExpiresAt - Date.now())
      );
    }
    connections.add(connection);
    connectionsByUser.set(userId, userConnections + 1);
    ws.on("message", async (data, isBinary) => {
      if (connection.revoked) return;
      const now = Date.now();
      if (now - connection.ingressWindowAt >= 1000) {
        connection.ingressWindowAt = now;
        connection.ingressFrames = 0;
        connection.ingressBytes = 0;
      }
      connection.ingressFrames += 1;
      connection.ingressBytes += data?.byteLength ?? data?.length ?? 0;
      if (
        connection.ingressFrames > MAX_CLIENT_FRAMES_PER_SECOND ||
        connection.ingressBytes > MAX_CLIENT_BYTES_PER_SECOND
      ) {
        terminateTerminalConnection(connection, 1008, "terminal ingress limit exceeded");
        return;
      }
      if (!isBinary) return protocolError(connection, "BINARY_REQUIRED", "RTP frames must be binary");
      let frame;
      try { frame = decodeFrame(data, DIRECTIONS.CLIENT_TO_SERVER); } catch (error) { return protocolError(connection, "INVALID_FRAME", error.message); }
      if (!connection.hello) {
        if (frame.type !== TYPES.HELLO || frame.streamId !== 0) return protocolError(connection, "HELLO_REQUIRED", "HELLO must be the first frame");
        let hello;
        try { hello = decodeJson(frame.payload); } catch (error) { return protocolError(connection, "INVALID_HELLO", error.message); }
        if (hello.clientVersion !== "1" || !Array.isArray(hello.capabilities) || !["binary", "multiplex", "history"].every((cap) => hello.capabilities.includes(cap)) || !verifyCsrf?.(request, hello.csrfToken)) return protocolError(connection, "INVALID_HELLO", "HELLO authentication or capabilities invalid");
        connection.hello = true;
        sendJson(connection, TYPES.HELLO_ACK, 0, { protocol: "RTP/1", heartbeatMs: HEARTBEAT_MS, maxUnackedBytes: MAX_UNACKED_BYTES });
        return;
      }
      try {
        if (frame.type === TYPES.OPEN && frame.streamId === 0) {
          if (
            connection.pendingOpens >= MAX_PENDING_OPENS_PER_CONNECTION ||
            connection.streams.size + connection.pendingOpens >= MAX_STREAMS_PER_CONNECTION ||
            activeOpenOperations >= MAX_ACTIVE_OPEN_OPERATIONS
          ) return protocolError(connection, "OPEN_LIMIT", "terminal stream limit exceeded");
          const reservation = reserveStream(connection);
          if (!reservation) return protocolError(connection, "STREAM_LIMIT", "terminal user stream limit exceeded");
          connection.pendingOpens += 1;
          activeOpenOperations += 1;
          try {
            return await openStream(connection, decodeJson(frame.payload), reservation);
          } finally {
            connection.pendingOpens -= 1;
            activeOpenOperations -= 1;
            if (!reservation.claimed) releaseStreamReservation(reservation);
          }
        }
        if (frame.type === TYPES.PING && frame.streamId === 0) {
          const now = Date.now();
          if (now - connection.lastClientPingAt < MIN_CLIENT_PING_INTERVAL_MS) {
            return protocolError(connection, "PING_RATE", "terminal PING rate exceeded");
          }
          connection.lastClientPingAt = now;
          return sendFrame(connection, TYPES.PONG, 0, frame.payload, 0, frame.sequence);
        }
        if (frame.type === TYPES.PONG && frame.streamId === 0) {
          if (!connection.lastServerPingSequence || frame.sequence !== connection.lastServerPingSequence) {
            return protocolError(connection, "PONG_SEQUENCE", "unexpected terminal PONG");
          }
          connection.lastServerPingSequence = 0;
          connection.lastPongAt = Date.now();
          connection.missedPongs = 0;
          return;
        }
        const stream = connection.streams.get(frame.streamId);
        if (!stream) return protocolError(connection, "UNKNOWN_STREAM", "unknown stream", frame.streamId);
        if (frame.type === TYPES.CLOSE_STREAM) return detachStream(connection, frame.streamId);
        if (frame.type === TYPES.INPUT) {
          if (!stream.ready || frame.sequence !== stream.inSequence + 1) return protocolError(connection, "INPUT_SEQUENCE", "non-contiguous input sequence", frame.streamId);
          stream.inSequence = frame.sequence;
          if (podMode) {
            const input = stream.inputDecoder.write(frame.payload);
            if (input) await sendPodInput(stream, input);
          } else stream.fallback.write(frame.payload);
          sendFrame(connection, TYPES.ACK, frame.streamId, EMPTY_BUFFER, 0, frame.sequence);
          if (Date.now() - stream.lastActivityAt >= 1000 && !stream.closed && stream.entry) {
            stream.lastActivityAt = Date.now();
            sessionActivity.set(sessionId(stream.project, stream.name), { lastInteractionAt: new Date(stream.lastActivityAt).toISOString() });
            broadcastSessionEvent("activity", stream.project, stream.entry);
          }
          return;
        }
        if (frame.type === TYPES.RESIZE) {
          const size = decodeResize(frame.payload);
          scheduleResizeSession(stream, size.cols, size.rows);
          return;
        }
        if (frame.type === TYPES.ACK) {
          if (frame.sequence < stream.lastAckSequence || frame.sequence > stream.outSequence) {
            return protocolError(connection, "ACK_SEQUENCE", "invalid cumulative ACK sequence", frame.streamId);
          }
          if (frame.sequence === stream.lastAckSequence) return;
          stream.lastAckSequence = frame.sequence;
          for (const [sequence, bytes] of stream.sentPayload) {
            if (sequence <= frame.sequence) {
              stream.unackedBytes -= bytes;
              stream.sentPayload.delete(sequence);
            } else {
              break;
            }
          }
          updateBackpressure(stream);
          return;
        }
        return protocolError(connection, "UNEXPECTED_FRAME", "frame type is not valid in this state", frame.streamId);
      } catch (error) {
        const message = String(error?.message || error || "terminal request failed").slice(0, 1024);
        if (frame.streamId && connection.streams.has(frame.streamId)) {
          try {
            sendJson(connection, TYPES.CLOSE_STREAM, frame.streamId, { code: "REQUEST_FAILED", message }, FLAGS.ERROR);
          } catch {
            // The stream still must be released even if the client disappeared mid-response.
          } finally {
            detachStream(connection, frame.streamId);
          }
        } else {
          protocolError(connection, "REQUEST_FAILED", message, frame.streamId);
        }
      }
    });
    ws.on("close", () => {
      for (const id of [...connection.streams.keys()]) detachStream(connection, id);
      releaseConnection(connection);
    });
    ws.on("error", () => {});
    connection.heartbeat = setInterval(() => {
      if (Date.now() - connection.lastPongAt >= HEARTBEAT_MS * 2) {
        terminateTerminalConnection(connection, 1001, "heartbeat timeout");
        return;
      }
      const payload = Buffer.alloc(8); payload.writeBigUInt64BE(BigInt(Date.now()));
      connection.lastServerPingSequence = sendFrame(connection, TYPES.PING, 0, payload) || 0;
      connection.missedPongs += 1;
    }, HEARTBEAT_MS);
    ws.on("close", () => {
      clearInterval(connection.heartbeat);
      clearTimeout(connection.authExpiry);
    });
  });
}

async function initLocalShells() {
  await fs.mkdir(SESSION_ARCHIVE_DIR, { recursive: true });
  await selectBackend();
  if (!podMode) return { backend: "subprocess", degraded: true, failures: ["POD_RUNTIME_UNAVAILABLE"] };
  let entries = [];
  try { entries = await fs.readdir(PROJECTS_ROOT, { withFileTypes: true }); } catch {}
  let warmed = 0;
  const failures = new Set();
  const verifiedProjects = new Set();
  for (const entry of entries.filter((candidate) => candidate.isDirectory() && !candidate.name.startsWith("."))) {
    try {
      const sessions = await readManifest(entry.name, { createDefault: false });
      await podRuntime.ensurePod(entry.name, projectRoot(entry.name));
      await detachOrphanViewerProcesses(entry.name);
      await preparePodSessions(entry.name, sessions, sessions, { podReady: true });
      warmed += sessions.length;
      verifiedProjects.add(entry.name);
    } catch (error) {
      if (error.code !== "ENOENT") {
        failures.add(entry.name);
        console.error(`[reaper] failed to warm ${entry.name}:`, error.message);
      }
    }
  }
  const caddy = await regenerateCaddyPorts({ quarantineInvalid: true, verifiedProjects });
  for (const project of caddy.quarantined) failures.add(project);
  return {
    backend: "pod",
    warmed,
    published: caddy.published,
    degraded: failures.size > 0,
    failures: [...failures].sort()
  };
}

async function shutdownLocalShells() {
  for (const connection of connections) {
    for (const id of [...connection.streams.keys()]) detachStream(connection, id);
    connection.ws.close(1001, "server shutting down");
  }
  await Promise.allSettled([...viewerCleanupOperations]);
  connections.clear();
  for (const fallback of fallbackSessions.values()) fallback.destroy();
  fallbackSessions.clear();
}

function __setPodRuntimeForTests(value) { podRuntime = value || defaultPodRuntime; }
function __createSubprocessSessionForTests(project = "test", name = "main") { return new SubprocessSession(project, { name, title: name, createdAt: nowIso() }); }

export {
  openProjectShell,
  listSessions,
  renameSession,
  destroySession,
  setProjectEnv,
  getProjectEnv,
  getProjectBashrc,
  setProjectBashrc,
  resetProjectState,
  attachTerminalWebSocket,
  closeTerminalConnectionsForSession,
  initLocalShells,
  shutdownLocalShells,
  selectBackend,
  listArchivedSessionLogs,
  stripTerminalQueries,
  getProjectPorts,
  updateProjectPorts,
  destroyProjectRuntime,
  validatePorts,
  regenerateCaddyPorts,
  validateShellEnvironment,
  setGlobalEnvProvider,
  refreshGlobalEnvironment,
  __setPodRuntimeForTests,
  __createSubprocessSessionForTests,
  SESSION_ARCHIVE_DIR as SESSION_ARCHIVE_PATH
};

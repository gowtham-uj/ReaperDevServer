import { execFile } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as nodePty from "node-pty";

export const POD_IMAGE = process.env.REAPER_POD_IMAGE || "reaper-pod:latest";
const POD_MEMORY_LIMIT = process.env.REAPER_POD_MEMORY_LIMIT || "8g";
const POD_CPU_LIMIT = process.env.REAPER_POD_CPU_LIMIT || "4";
const POD_PIDS_LIMIT = process.env.REAPER_POD_PIDS_LIMIT || "4096";
export const POD_NETWORK_POOL = process.env.REAPER_POD_NETWORK_POOL || "10.240.0.0/16";
export const TMUX_SOCKET = "/reaper/tmux.sock";
export const CADDY_CONTAINER = process.env.CADDY_CONTAINER || "reaper-caddy";

const GIB = 1024 ** 3;
const OUTPUT_LIMIT = 64 * 1024;
const DOCKER_TIMEOUT_MS = 30_000;
const MAX_DOCKER_TIMEOUT_MS = 5 * 60_000;
const MAX_ACTIVE_DOCKER_COMMANDS = 8;
const MAX_QUEUED_DOCKER_COMMANDS = 32;
const DOCKER_QUEUE_TIMEOUT_MS = 10_000;
const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const NETWORK_ICC_OPTION = "com.docker.network.bridge.enable_icc";
const DEFAULT_DATA_ROOT = path.resolve(MODULE_DIR, "..", "..", "..", "..", "tmp");

let runtime = {
  commandRunner: runCommand,
  ptySpawn: nodePty.spawn,
  stateDir: process.env.STATE_DIR || path.resolve(process.cwd(), ".reaper-local"),
  projectsRoot: process.env.VPS_PROJECTS || path.join(DEFAULT_DATA_ROOT, "vps-projects"),
  hostProjectsRoot: process.env.HOST_PROJECTS_ROOT || ""
};
let networkOperation = Promise.resolve();
let allocationOperation = Promise.resolve();
const projectOperations = new Map();
const ensureOperations = new Map();
let activeDockerCommands = 0;
const dockerCommandWaiters = [];

function releaseDockerCommand() {
  const waiter = dockerCommandWaiters.shift();
  if (waiter) {
    clearTimeout(waiter.timer);
    waiter.resolve(releaseDockerCommand);
  } else {
    activeDockerCommands = Math.max(0, activeDockerCommands - 1);
  }
}

function acquireDockerCommand() {
  if (activeDockerCommands < MAX_ACTIVE_DOCKER_COMMANDS) {
    activeDockerCommands += 1;
    return Promise.resolve(releaseDockerCommand);
  }
  if (dockerCommandWaiters.length >= MAX_QUEUED_DOCKER_COMMANDS) {
    return Promise.reject(new Error("Docker command capacity is busy"));
  }
  return new Promise((resolve, reject) => {
    const waiter = { resolve, timer: null };
    waiter.timer = setTimeout(() => {
      const index = dockerCommandWaiters.indexOf(waiter);
      if (index >= 0) dockerCommandWaiters.splice(index, 1);
      reject(new Error("Docker command queue timed out"));
    }, DOCKER_QUEUE_TIMEOUT_MS);
    dockerCommandWaiters.push(waiter);
  });
}

function bounded(value, limit = OUTPUT_LIMIT) {
  const text = String(value ?? "");
  if (Buffer.byteLength(text) <= limit) return text;
  return `${Buffer.from(text).subarray(0, limit).toString("utf8")}\n[output truncated]`;
}

function runCommand(file, args, options = {}) {
  return new Promise((resolve) => {
    const { input, ...execOptions } = options;
    const maxBuffer = Number.isSafeInteger(execOptions.maxBuffer) ? execOptions.maxBuffer : OUTPUT_LIMIT;
    const timeout = Number.isSafeInteger(execOptions.timeout) ? execOptions.timeout : DOCKER_TIMEOUT_MS;
    const child = execFile(file, args, {
      encoding: "utf8",
      maxBuffer,
      windowsHide: true,
      timeout,
      killSignal: "SIGKILL",
      ...execOptions
    }, (error, stdout, stderr) => {
      resolve({
        code: error ? (Number.isInteger(error.code) ? error.code : 1) : 0,
        stdout: bounded(stdout, maxBuffer),
        stderr: bounded(stderr || (error && !Number.isInteger(error.code) ? error.message : ""), maxBuffer)
      });
    });
    if (input !== undefined) child.stdin.end(String(input));
  });
}

async function docker(args, { allowFailure = false, input, maxBuffer = OUTPUT_LIMIT, timeoutMs = DOCKER_TIMEOUT_MS } = {}) {
  const release = await acquireDockerCommand();
  try {
    const commandOptions = { maxBuffer, timeout: timeoutMs };
    if (input !== undefined) commandOptions.input = input;
    const result = await runtime.commandRunner("docker", args, commandOptions);
    const normalized = {
      code: Number(result?.code ?? 0),
      stdout: bounded(result?.stdout, maxBuffer),
      stderr: bounded(result?.stderr, maxBuffer)
    };
    if (!allowFailure && normalized.code !== 0) {
      const detail = normalized.stderr.trim() || normalized.stdout.trim() || `exit code ${normalized.code}`;
      throw new Error(`docker ${args[0]} failed: ${detail}`);
    }
    return normalized;
  } finally {
    release();
  }
}

export const PROJECT_NAME_RE = /^(?=.*[A-Za-z0-9])[A-Za-z0-9_. -]{1,64}$/;

export function validateProjectName(project) {
  if (typeof project !== "string" || !PROJECT_NAME_RE.test(project)) {
    throw new TypeError("invalid project name");
  }
  return project;
}

function validateProjectPath(project, projectPath) {
  if (typeof projectPath !== "string" || !projectPath || projectPath.includes("\0")) {
    throw new TypeError("invalid project path");
  }
  const expected = path.resolve(runtime.projectsRoot, project);
  const actual = path.resolve(projectPath);
  if (path.dirname(expected) !== path.resolve(runtime.projectsRoot) || actual !== expected) {
    throw new TypeError("project path must be the project's direct directory");
  }
}

function hostProjectPath(project) {
  if (!runtime.hostProjectsRoot) throw new Error("HOST_PROJECTS_ROOT is required for project pods");
  if (!path.isAbsolute(runtime.hostProjectsRoot)) throw new Error("HOST_PROJECTS_ROOT must be absolute");
  const root = path.resolve(runtime.hostProjectsRoot);
  const target = path.resolve(root, project);
  if (path.dirname(target) !== root) throw new TypeError("invalid host project path");
  return target;
}

export function podName(project) {
  validateProjectName(project);
  const sanitized = project.toLowerCase().replace(/[^a-z0-9-]/g, "-").replace(/-+/g, "-").replace(/^-|-$/g, "");
  if (!sanitized || !/[a-z0-9]/.test(sanitized)) throw new TypeError("project name cannot form a pod name");
  const digest = crypto.createHash("sha256").update(project, "utf8").digest("hex").slice(0, 10);
  return `reaper-pod-${sanitized.slice(0, 40)}-${digest}`;
}

function parseInspect(stdout) {
  let parsed;
  try {
    parsed = JSON.parse(stdout);
  } catch {
    throw new Error("docker inspect returned invalid JSON");
  }
  if (!Array.isArray(parsed) || parsed.length !== 1 || !parsed[0] || typeof parsed[0] !== "object") {
    throw new Error("docker inspect returned an unexpected result");
  }
  return parsed[0];
}

function securityState(data) {
  const capDrop = Array.isArray(data.HostConfig?.CapDrop) ? data.HostConfig.CapDrop : [];
  const securityOpt = Array.isArray(data.HostConfig?.SecurityOpt) ? data.HostConfig.SecurityOpt : [];
  const normalized = (cap) => String(cap).toUpperCase().replace(/^CAP_/, "");
  return {
    netRawDropped: capDrop.some((capability) => normalized(capability) === "NET_RAW"),
    noNewPrivileges: securityOpt.some((option) => /^no-new-privileges(?::true)?$/i.test(String(option)))
  };
}

function inspectState(project, data) {
  const network = podNetworkName(project);
  const security = securityState(data);
  const attachedNetworks = Object.keys(data.NetworkSettings?.Networks || {});
  const ip = data.NetworkSettings?.Networks?.[network]?.IPAddress || null;
  return {
    exists: true,
    running: Boolean(data.State?.Running),
    ip,
    isolated: Boolean(ip) && attachedNetworks.length === 1 && attachedNetworks[0] === network,
    legacySecurity: !security.netRawDropped || !security.noNewPrivileges
  };
}

async function inspectContainer(project) {
  const result = await docker(["inspect", podName(project)], { allowFailure: true });
  if (result.code !== 0) {
    const detail = result.stderr.trim() || result.stdout.trim() || `exit code ${result.code}`;
    if (/no such (?:object|container)/i.test(detail)) {
      return { exists: false, running: false, ip: null, isolated: false, legacySecurity: false, data: null };
    }
    throw new Error(`docker inspect failed: ${detail}`);
  }
  const data = parseInspect(result.stdout);
  return { ...inspectState(project, data), data };
}

export async function podAvailable() {
  try {
    const result = await docker(["version"], { allowFailure: true });
    if (result.code === 0) return { available: true };
    return { available: false, reason: result.stderr.trim() || result.stdout.trim() || "docker daemon unavailable" };
  } catch (error) {
    return { available: false, reason: bounded(error?.message || error) };
  }
}

function ipv4ToInteger(address) {
  const octets = String(address).split(".");
  if (octets.length !== 4 || octets.some((octet) => !/^(?:0|[1-9]\d{0,2})$/.test(octet) || Number(octet) > 255)) {
    throw new Error(`invalid IPv4 address ${address}`);
  }
  return octets.reduce((value, octet) => value * 256 + Number(octet), 0);
}

function integerToIpv4(value) {
  return [
    Math.floor(value / 2 ** 24),
    Math.floor(value / 2 ** 16) % 256,
    Math.floor(value / 2 ** 8) % 256,
    value % 256
  ].join(".");
}

function cidrRange(cidr) {
  const match = /^([^/]+)\/(\d{1,2})$/.exec(String(cidr));
  if (!match) return null;
  const prefix = Number(match[2]);
  if (prefix < 0 || prefix > 32) return null;
  let address;
  try { address = ipv4ToInteger(match[1]); }
  catch { return null; }
  const size = 2 ** (32 - prefix);
  const start = Math.floor(address / size) * size;
  return { start, end: start + size - 1 };
}

function networkPool() {
  const match = /^([^/]+)\/(\d{1,2})$/.exec(POD_NETWORK_POOL);
  if (!match) throw new Error("REAPER_POD_NETWORK_POOL must be an IPv4 CIDR");
  const base = ipv4ToInteger(match[1]);
  const prefix = Number(match[2]);
  if (prefix < 8 || prefix > 29) throw new Error("REAPER_POD_NETWORK_POOL prefix must be between /8 and /29");
  const size = 2 ** (32 - prefix);
  if (base % size !== 0) throw new Error("REAPER_POD_NETWORK_POOL must use its canonical network address");
  const end = base + size - 1;
  const privateRange = (
    (base >= ipv4ToInteger("10.0.0.0") && end <= ipv4ToInteger("10.255.255.255")) ||
    (base >= ipv4ToInteger("172.16.0.0") && end <= ipv4ToInteger("172.31.255.255")) ||
    (base >= ipv4ToInteger("192.168.0.0") && end <= ipv4ToInteger("192.168.255.255"))
  );
  if (!privateRange) throw new Error("REAPER_POD_NETWORK_POOL must be wholly contained in an RFC1918 range");
  return { base, slots: size / 8 };
}

function projectSubnetIndex(project, slots) {
  return crypto.createHash("sha256").update(project, "utf8").digest().readUInt32BE(0) % slots;
}

function subnetAt(pool, index) {
  return `${integerToIpv4(pool.base + index * 8)}/29`;
}

function parseInspectArray(stdout, subject) {
  let parsed;
  try { parsed = JSON.parse(stdout); }
  catch { throw new Error(`${subject} returned invalid JSON`); }
  if (!Array.isArray(parsed)) throw new Error(`${subject} returned an unexpected result`);
  return parsed;
}

async function occupiedNetworkSubnets(excludedName = null) {
  const listed = await docker(["network", "ls", "--quiet"]);
  const ids = listed.stdout.split(/\r?\n/).map((id) => id.trim()).filter(Boolean);
  const occupied = [];
  for (let offset = 0; offset < ids.length; offset += 100) {
    const inspected = await docker(
      ["network", "inspect", ...ids.slice(offset, offset + 100)],
      { maxBuffer: 1024 * 1024 }
    );
    for (const network of parseInspectArray(inspected.stdout, "docker network inventory inspect")) {
      if (network?.Name === excludedName) continue;
      const configs = network?.IPAM?.Config;
      if (!Array.isArray(configs)) continue;
      for (const config of configs) {
        const range = cidrRange(config?.Subnet);
        if (range) occupied.push(range);
      }
    }
  }
  return occupied;
}

async function selectProjectSubnet(project, excludedName = null) {
  const pool = networkPool();
  const occupied = await occupiedNetworkSubnets(excludedName);
  const first = projectSubnetIndex(project, pool.slots);
  for (let probe = 0; probe < pool.slots; probe += 1) {
    const index = (first + probe) % pool.slots;
    const start = pool.base + index * 8;
    if (!occupied.some((range) => range.start <= start + 7 && range.end >= start)) {
      return subnetAt(pool, index);
    }
  }
  throw new Error(`project pod network pool ${POD_NETWORK_POOL} is exhausted`);
}

export function podNetworkName(project) {
  validateProjectName(project);
  const suffix = podName(project).slice("reaper-pod-".length);
  return `reaper-net-${suffix}`;
}

function parseNetworkInspect(networkName, stdout) {
  let inspected;
  try { inspected = JSON.parse(stdout); }
  catch { throw new Error(`${networkName} network inspect returned invalid JSON`); }
  const network = Array.isArray(inspected) && inspected.length === 1 ? inspected[0] : null;
  if (!network || typeof network !== "object") {
    throw new Error(`${networkName} network inspect returned an unexpected result`);
  }
  return network;
}

function assertNetworkConfiguration(project, stdout, expectedSubnet) {
  const networkName = podNetworkName(project);
  const network = parseNetworkInspect(networkName, stdout);
  const driverMatches = network.Driver === "bridge";
  const iccDisabled = String(network.Options?.[NETWORK_ICC_OPTION] || "").toLowerCase() === "false";
  const ownerMatches = network.Labels?.["reaper.project"] === project;
  const ipam = network.IPAM?.Config;
  const ipamMatches = Array.isArray(ipam) && ipam.length === 1 && ipam[0]?.Subnet === expectedSubnet;
  if (!driverMatches || !iccDisabled || !ownerMatches || !ipamMatches) {
    throw new Error(`${networkName} must be a labelled bridge for ${project} with ICC disabled and exact subnet ${expectedSubnet}`);
  }
  return network;
}

async function inspectNetwork(project, { allowMissing = false, expectedSubnet } = {}) {
  const networkName = podNetworkName(project);
  const inspected = await docker(["network", "inspect", networkName], { allowFailure: true });
  if (inspected.code === 0) {
    const subnet = expectedSubnet || await selectProjectSubnet(project, networkName);
    return assertNetworkConfiguration(project, inspected.stdout, subnet);
  }
  const detail = inspected.stderr.trim() || inspected.stdout.trim();
  if (allowMissing && /(?:not found|no such network)/i.test(detail)) return null;
  throw new Error(`docker network inspect failed: ${detail || `exit code ${inspected.code}`}`);
}

async function ensureNetwork(project) {
  const operation = networkOperation.then(async () => {
    const existing = await inspectNetwork(project, { allowMissing: true });
    if (existing) return;
    const networkName = podNetworkName(project);
    const pool = networkPool();
    for (let attempt = 0; attempt < pool.slots; attempt += 1) {
      const subnet = await selectProjectSubnet(project);
      const created = await docker([
        "network", "create", "--driver", "bridge", "--subnet", subnet,
        "--opt", `${NETWORK_ICC_OPTION}=false`,
        "--label", `reaper.project=${project}`, networkName
      ], { allowFailure: true });
      if (created.code === 0) {
        await inspectNetwork(project, { expectedSubnet: subnet });
        return;
      }
      const raced = await inspectNetwork(project, { allowMissing: true });
      if (raced) return;
      const detail = created.stderr.trim() || created.stdout.trim();
      if (!/(?:overlap|conflict|already exists|pool overlaps)/i.test(detail)) {
        throw new Error(`docker network create failed: ${detail || `exit code ${created.code}`}`);
      }
    }
    throw new Error(`project pod network pool ${POD_NETWORK_POOL} is exhausted`);
  });
  networkOperation = operation.catch(() => {});
  return operation;
}

async function readAllocations() {
  const file = path.join(runtime.stateDir, "pods.json");
  try {
    const parsed = JSON.parse(await fs.readFile(file, "utf8"));
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch (error) {
    if (error.code === "ENOENT") return {};
    throw new Error(`cannot read pod IP state: ${error.message}`);
  }
}

async function writeAllocations(allocations) {
  await fs.mkdir(runtime.stateDir, { recursive: true });
  const destination = path.join(runtime.stateDir, "pods.json");
  const temporary = path.join(runtime.stateDir, `.pods.json.${process.pid}.${Date.now()}.tmp`);
  const handle = await fs.open(temporary, "wx", 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(allocations, null, 2)}\n`, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    await fs.rename(temporary, destination);
  } catch (error) {
    await fs.rm(temporary, { force: true });
    throw error;
  }
}


async function releaseIp(project) {
  const operation = allocationOperation.then(async () => {
    const allocations = await readAllocations();
    if (!Object.hasOwn(allocations, project)) return;
    delete allocations[project];
    await writeAllocations(allocations);
  });
  allocationOperation = operation.catch(() => {});
  return operation;
}

async function releaseLegacyAllocation(project) {
  try {
    await releaseIp(project);
  } catch {
    // Static IP state predates private networks and must never gate authoritative teardown.
  }
}

function serializeProject(project, work) {
  const previous = projectOperations.get(project) || Promise.resolve();
  const operation = previous.catch(() => {}).then(work);
  projectOperations.set(project, operation);
  operation.finally(() => {
    if (projectOperations.get(project) === operation) projectOperations.delete(project);
  }).catch(() => {});
  return operation;
}

function expectedResourceConfiguration() {
  const memoryMatch = /^(\d+)([kmgt]?)$/i.exec(POD_MEMORY_LIMIT);
  if (!memoryMatch) throw new Error("REAPER_POD_MEMORY_LIMIT must be an integer with an optional binary unit");
  const unit = memoryMatch[2].toLowerCase();
  const multiplier = { "": 1, k: 1024, m: 1024 ** 2, g: GIB, t: 1024 ** 4 }[unit];
  const memory = Number(memoryMatch[1]) * multiplier;
  const nanoCpus = Number(POD_CPU_LIMIT) * 1_000_000_000;
  const pidsLimit = Number(POD_PIDS_LIMIT);
  if (!Number.isSafeInteger(memory) || memory <= 0 ||
      !Number.isSafeInteger(nanoCpus) || nanoCpus <= 0 ||
      !Number.isSafeInteger(pidsLimit) || pidsLimit <= 0) {
    throw new Error("pod resource limits must be positive integers");
  }
  return { memory, nanoCpus, pidsLimit };
}

function validateImmutableConfiguration(project, data) {
  const owner = data.Config?.Labels?.["reaper.project"];
  if (owner !== project) throw new Error(`pod name collision for ${podName(project)}`);
  if (data.Config?.Image !== POD_IMAGE) {
    throw new Error(`existing pod for ${project} uses an unexpected image`);
  }
  if (data.Config?.WorkingDir !== "/work") {
    throw new Error(`existing pod for ${project} uses an unexpected working directory`);
  }
  const expectedBind = `${hostProjectPath(project)}:/work`;
  const binds = Array.isArray(data.HostConfig?.Binds) ? data.HostConfig.Binds : [];
  if (!binds.includes(expectedBind)) {
    throw new Error(`existing pod for ${project} uses an unexpected workspace bind`);
  }
}

function mutableUpdateArgs(data) {
  const expected = expectedResourceConfiguration();
  const args = ["update"];
  if (data.HostConfig?.RestartPolicy?.Name !== "unless-stopped") {
    args.push("--restart", "unless-stopped");
  }
  if (data.HostConfig?.Memory !== expected.memory || data.HostConfig?.MemorySwap !== expected.memory) {
    args.push("--memory", POD_MEMORY_LIMIT, "--memory-swap", POD_MEMORY_LIMIT);
  }
  if (data.HostConfig?.NanoCpus !== expected.nanoCpus) args.push("--cpus", POD_CPU_LIMIT);
  if (data.HostConfig?.PidsLimit !== expected.pidsLimit) args.push("--pids-limit", POD_PIDS_LIMIT);
  return args;
}

function assertMutableConfiguration(data) {
  const expected = expectedResourceConfiguration();
  if (data.HostConfig?.RestartPolicy?.Name !== "unless-stopped" ||
      data.HostConfig?.Memory !== expected.memory ||
      data.HostConfig?.MemorySwap !== expected.memory ||
      data.HostConfig?.NanoCpus !== expected.nanoCpus ||
      data.HostConfig?.PidsLimit !== expected.pidsLimit) {
    throw new Error("docker update did not apply the required pod resource and restart configuration");
  }
}

async function updateMutableConfiguration(project, inspected) {
  const args = mutableUpdateArgs(inspected.data);
  if (args.length === 1) {
    assertMutableConfiguration(inspected.data);
    return inspected;
  }
  await docker([...args, podName(project)]);
  const updated = await inspectContainer(project);
  if (!updated.exists) throw new Error(`pod ${podName(project)} disappeared during update`);
  validateImmutableConfiguration(project, updated.data);
  assertMutableConfiguration(updated.data);
  return updated;
}

async function migrateNetworks(project, inspected) {
  const networkName = podNetworkName(project);
  const attached = inspected.data?.NetworkSettings?.Networks || {};
  if (!Object.hasOwn(attached, networkName)) {
    await docker(["network", "connect", networkName, podName(project)]);
    inspected = await inspectContainer(project);
  }

  if (inspected.running && !inspected.ip) {
    throw new Error(`pod ${podName(project)} has no IP on its private network`);
  }

  const otherNetworks = Object.keys(inspected.data?.NetworkSettings?.Networks || {})
    .filter((name) => name !== networkName);
  for (const name of otherNetworks) {
    await docker(["network", "disconnect", name, podName(project)]);
  }
  inspected = await inspectContainer(project);
  const remaining = Object.keys(inspected.data?.NetworkSettings?.Networks || {});
  if (remaining.length !== 1 || remaining[0] !== networkName) {
    throw new Error(`pod ${podName(project)} is not exclusively attached to its private network`);
  }
  if (inspected.running && !inspected.ip) {
    throw new Error(`pod ${podName(project)} has no IP on its private network`);
  }
  return inspected;
}

export async function ensurePod(project, projectPath) {
  validateProjectName(project);
  validateProjectPath(project, projectPath);
  const inFlight = ensureOperations.get(project);
  if (inFlight) return inFlight;
  const operation = serializeProject(project, async () => {
    await ensureNetwork(project);
    let existing = await inspectContainer(project);
    if (existing.exists) {
      validateImmutableConfiguration(project, existing.data);
      existing = await updateMutableConfiguration(project, existing);
      existing = await migrateNetworks(project, existing);
      if (!existing.running) {
        await docker(["start", podName(project)]);
        existing = await inspectContainer(project);
      }
      existing = await migrateNetworks(project, existing);
      validateImmutableConfiguration(project, existing.data);
      assertMutableConfiguration(existing.data);
      if (existing.legacySecurity) {
        const networks = Object.keys(existing.data?.NetworkSettings?.Networks || {});
        if (networks.length !== 1 || networks[0] !== podNetworkName(project) || !existing.ip) {
          throw new Error(`legacy pod ${podName(project)} is not exclusively isolated`);
        }
      }
      return {
        name: podName(project),
        ip: existing.ip,
        generation: existing.data?.Id || null,
        legacySecurity: existing.legacySecurity
      };
    }
    await docker([
      "run", "-d", "--name", podName(project), "--restart", "unless-stopped",
      "--memory", POD_MEMORY_LIMIT, "--memory-swap", POD_MEMORY_LIMIT,
      "--cpus", POD_CPU_LIMIT, "--pids-limit", POD_PIDS_LIMIT,
      "--cap-drop", "NET_RAW", "--security-opt", "no-new-privileges",
      "--network", podNetworkName(project), "--hostname", podName(project).slice("reaper-pod-".length),
      "-v", `${hostProjectPath(project)}:/work`, "-w", "/work",
      "--label", `reaper.project=${project}`, POD_IMAGE
    ]);
    const created = await inspectContainer(project);
    if (!created.exists || !created.running || !created.ip) {
      throw new Error(`new pod ${podName(project)} did not start on its private network`);
    }
    validateImmutableConfiguration(project, created.data);
    assertMutableConfiguration(created.data);
    if (created.legacySecurity) throw new Error(`new pod ${podName(project)} is missing required security flags`);
    return {
      name: podName(project),
      ip: created.ip,
      generation: created.data?.Id || null,
      legacySecurity: false
    };
  });
  ensureOperations.set(project, operation);
  operation.finally(() => {
    if (ensureOperations.get(project) === operation) ensureOperations.delete(project);
  }).catch(() => {});
  return operation;
}

export async function podExec(project, argv, opts = {}) {
  validateProjectName(project);
  if (!Array.isArray(argv) || argv.length === 0 || argv.some((arg) => typeof arg !== "string" || arg.includes("\0"))) {
    throw new TypeError("pod exec argv must be a non-empty string array");
  }
  if (opts.input !== undefined && typeof opts.input !== "string") throw new TypeError("pod exec input must be a string");
  const maxBuffer = opts.maxBuffer === undefined ? OUTPUT_LIMIT : Number(opts.maxBuffer);
  if (!Number.isSafeInteger(maxBuffer) || maxBuffer < OUTPUT_LIMIT || maxBuffer > 64 * 1024 * 1024) {
    throw new TypeError("pod exec maxBuffer must be between 64 KiB and 64 MiB");
  }
  const timeoutMs = opts.timeoutMs === undefined ? DOCKER_TIMEOUT_MS : Number(opts.timeoutMs);
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 100 || timeoutMs > MAX_DOCKER_TIMEOUT_MS) {
    throw new TypeError("pod exec timeoutMs must be between 100 ms and 5 minutes");
  }
  return docker(["exec", "-i", podName(project), ...argv], { allowFailure: true, input: opts.input, maxBuffer, timeoutMs });
}

export function podExecPty(project, argv, { cols, rows }) {
  validateProjectName(project);
  if (!Array.isArray(argv) || argv.length === 0 || argv.some((arg) => typeof arg !== "string" || arg.includes("\0"))) {
    throw new TypeError("pod PTY argv must be a non-empty string array");
  }
  if (!Number.isInteger(cols) || cols < 1 || !Number.isInteger(rows) || rows < 1) {
    throw new TypeError("PTY dimensions must be positive integers");
  }
  return runtime.ptySpawn("docker", ["exec", "-it", podName(project), ...argv], {
    name: "xterm-256color",
    cols,
    rows,
    cwd: runtime.projectsRoot,
    env: process.env
  });
}

export async function podInspect(project) {
  validateProjectName(project);
  const inspected = await inspectContainer(project);
  const result = {
    running: inspected.running,
    ip: inspected.ip,
    exists: inspected.exists,
    isolated: inspected.isolated,
    legacySecurity: inspected.legacySecurity
  };
  if (!inspected.exists) return result;
  const owner = inspected.data?.Config?.Labels?.["reaper.project"];
  if (owner !== project) throw new Error(`pod name collision for ${podName(project)}`);
  validateImmutableConfiguration(project, inspected.data);
  return {
    ...result,
    generation: typeof inspected.data?.Id === "string" ? inspected.data.Id : null
  };
}

export async function destroyPod(project) {
  validateProjectName(project);
  ensureOperations.delete(project);
  return serializeProject(project, async () => {
    const existing = await inspectContainer(project);
    if (existing.exists) {
      const owner = existing.data?.Config?.Labels?.["reaper.project"];
      if (owner !== project) throw new Error(`pod name collision for ${podName(project)}`);
    }
    const network = await inspectNetwork(project, { allowMissing: true });
    await releaseLegacyAllocation(project);
    if (existing.exists) {
      if (existing.running) await docker(["stop", podName(project)]);
      await docker(["rm", podName(project)]);
    }
    if (network) await docker(["network", "rm", podNetworkName(project)]);
  });
}

export async function listPods() {
  const result = await docker(["ps", "-a", "--filter", "label=reaper.project", "--format", "{{.Names}}"]);
  const names = result.stdout.split(/\r?\n/).map((name) => name.trim()).filter(Boolean);
  const pods = [];
  for (const name of names) {
    const inspected = await docker(["inspect", name], { allowFailure: true });
    if (inspected.code !== 0) continue;
    const data = parseInspect(inspected.stdout);
    const project = data.Config?.Labels?.["reaper.project"];
    if (typeof project !== "string") continue;
    const state = inspectState(project, data);
    pods.push({ project, name, running: state.running, ip: state.ip, legacySecurity: state.legacySecurity });
  }
  return pods.sort((a, b) => a.project.localeCompare(b.project));
}

export async function reloadCaddy() {
  const result = await docker([
    "exec", CADDY_CONTAINER, "caddy", "reload", "--config", "/etc/caddy/Caddyfile"
  ], { allowFailure: true });
  if (result.code !== 0) {
    const detail = result.stderr.trim() || result.stdout.trim() || `exit code ${result.code}`;
    throw new Error(`Caddy reload failed: ${detail}`);
  }
}

export const __testing = Object.freeze({
  configure(overrides = {}) {
    runtime = { ...runtime, ...overrides };
    networkOperation = Promise.resolve();
    allocationOperation = Promise.resolve();
    projectOperations.clear();
    ensureOperations.clear();
  },
  reset() {
    runtime = {
      commandRunner: runCommand,
      ptySpawn: nodePty.spawn,
      stateDir: process.env.STATE_DIR || path.resolve(process.cwd(), ".reaper-local"),
      projectsRoot: process.env.VPS_PROJECTS || path.join(DEFAULT_DATA_ROOT, "vps-projects"),
      hostProjectsRoot: process.env.HOST_PROJECTS_ROOT || ""
    };
    networkOperation = Promise.resolve();
    allocationOperation = Promise.resolve();
    projectOperations.clear();
    ensureOperations.clear();
  }
});

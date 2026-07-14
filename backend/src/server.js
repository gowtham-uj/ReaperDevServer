/* Reaper V2 backend - lean. No docker, no postgres, no theia, no hermes. */
import http from "node:http";
import fs from "node:fs/promises";
import fss from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";
import { z } from "zod";
import jwt from "jsonwebtoken";
import bcrypt from "bcryptjs";
import { openProjectShell, listSessions, renameSession, destroySession, destroyProjectRuntime, setProjectEnv, getProjectEnv, getProjectBashrc, setProjectBashrc, resetProjectState, attachTerminalWebSocket, closeTerminalConnectionsForSession, initLocalShells, shutdownLocalShells, listArchivedSessionLogs, getProjectPorts, updateProjectPorts, validateShellEnvironment, setGlobalEnvProvider, refreshGlobalEnvironment, SESSION_ARCHIVE_PATH } from "./services/local-shell.js";
import { destroyPod, PROJECT_NAME_RE } from "./services/pod-runtime.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "..", "..");
const VPS_PROJECTS = process.env.VPS_PROJECTS || path.resolve(REPO_ROOT, "..", "tmp", "vps-projects");
const GLOBAL_ENV = process.env.GLOBAL_ENV || path.resolve(REPO_ROOT, "..", "tmp", "global-env.json");
const STATE_DIR = process.env.STATE_DIR || path.join(process.cwd(), ".reaper-local");


for (const d of [VPS_PROJECTS, STATE_DIR, path.dirname(GLOBAL_ENV)]) {
  try { await fs.mkdir(d, { recursive: true }); } catch {}
}

const PORT = Number(process.env.REAPER_PORT || process.env.PORT || 4000);
function requiredSecret(name) {
  const value = String(process.env[name] || "").trim();
  if (value.length < 32 || /(?:change-me|change_me|placeholder|example-secret)/i.test(value)) {
    throw new Error(`${name} must be a non-placeholder secret of at least 32 characters`);
  }
  return value;
}
const ACCESS_SECRET = requiredSecret("JWT_ACCESS_SECRET");
const ADMIN_USER = String(process.env.APP_ADMIN_USERNAME || "").trim();
const ADMIN_PASS = String(process.env.APP_ADMIN_PASSWORD || "");
const COOKIE_SECURE = process.env.COOKIE_SECURE === "true" || (process.env.NODE_ENV === "production" && process.env.COOKIE_SECURE !== "false");
if (process.env.NODE_ENV === "production" && !COOKIE_SECURE) throw new Error("COOKIE_SECURE cannot be disabled in production");
const DOMAIN_RE = /^(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,63}$/;
const APEX_DOMAIN = String(process.env.APEX_DOMAIN || "").trim().toLowerCase().replace(/^\./, "");
const CONFIGURED_COOKIE_DOMAIN = String(process.env.COOKIE_DOMAIN || "").trim().toLowerCase().replace(/^\./, "");
if (APEX_DOMAIN && !DOMAIN_RE.test(APEX_DOMAIN)) throw new Error("APEX_DOMAIN must be a valid apex DNS domain");
if (CONFIGURED_COOKIE_DOMAIN && !DOMAIN_RE.test(CONFIGURED_COOKIE_DOMAIN)) throw new Error("COOKIE_DOMAIN must be a valid apex DNS domain");
if (APEX_DOMAIN && CONFIGURED_COOKIE_DOMAIN && CONFIGURED_COOKIE_DOMAIN !== APEX_DOMAIN) {
  throw new Error("COOKIE_DOMAIN must match APEX_DOMAIN so published-port authentication remains scoped to this deployment");
}
const COOKIE_DOMAIN = CONFIGURED_COOKIE_DOMAIN || APEX_DOMAIN;
const COOKIE_BASE = `Path=/; HttpOnly; SameSite=Lax${COOKIE_SECURE ? "; Secure" : ""}${COOKIE_DOMAIN ? `; Domain=${COOKIE_DOMAIN}` : ""}`;
const JSON_BODY_MAX_BYTES = Number(process.env.JSON_BODY_MAX_BYTES || 1024 * 1024);
if (!Number.isSafeInteger(JSON_BODY_MAX_BYTES) || JSON_BODY_MAX_BYTES < 1024) throw new Error("JSON_BODY_MAX_BYTES must be an integer of at least 1024");

/* ---------- in-memory + file-backed state ---------- */
const state = {
  users: [],
  sessions: [],
  tokens: []
};
const AUTH_SESSIONS_FILE = path.join(STATE_DIR, "auth-sessions.json");
const AUTH_SESSION_TTL_MS = 8 * 60 * 60 * 1000;
const MAX_AUTH_SESSIONS_PER_USER = 16;
let authSessions = new Map();
let authSessionOperation = Promise.resolve();
let localShellStatus = { backend: "starting", degraded: true, failures: ["STARTING"] };
async function loadJson(p, fallback) {
  try { return JSON.parse(await fs.readFile(p, "utf8")); } catch { return fallback; }
}
async function saveJson(p, v) { try { await fs.writeFile(p, JSON.stringify(v, null, 2)); } catch {} }

await (async () => {
  state.users = await loadJson(path.join(STATE_DIR, "users.json"), []);
  state.sessions = await loadJson(path.join(STATE_DIR, "sessions.json"), []);
  state.tokens = await loadJson(path.join(STATE_DIR, "tokens.json"), []);
  const now = Date.now();
  const storedAuthSessions = await loadJson(AUTH_SESSIONS_FILE, []);
  authSessions = new Map(
    (Array.isArray(storedAuthSessions) ? storedAuthSessions : [])
      .filter((item) =>
        item && typeof item.id === "string" && /^[0-9a-f-]{36}$/i.test(item.id) &&
        typeof item.userId === "string" && Number.isSafeInteger(item.expiresAt) && item.expiresAt > now
      )
      .map((item) => [item.id, {
        id: item.id,
        userId: item.userId,
        createdAt: Number.isSafeInteger(item.createdAt) ? item.createdAt : now,
        expiresAt: item.expiresAt
      }])
  );
  if (!state.users.length) {
    if (!/^[A-Za-z0-9_.-]{3,64}$/.test(ADMIN_USER)) {
      throw new Error("APP_ADMIN_USERNAME is required on first boot and must be 3–64 safe characters");
    }
    if (ADMIN_PASS.length < 12 || /(?:change-me|change_me|placeholder|example-password)/i.test(ADMIN_PASS)) {
      throw new Error("APP_ADMIN_PASSWORD is required on first boot and must be a non-placeholder value of at least 12 characters");
    }
    state.users.push({
      id: crypto.randomUUID(),
      username: ADMIN_USER,
      role: "owner",
      password_hash: bcrypt.hashSync(ADMIN_PASS, 12),
      created_at: new Date().toISOString()
    });
    await saveJson(path.join(STATE_DIR, "users.json"), state.users);
  }
})();

async function writeAuthSessions(records) {
  const temporary = path.join(STATE_DIR, `.auth-sessions-${crypto.randomBytes(8).toString("hex")}.tmp`);
  let handle;
  try {
    handle = await fs.open(temporary, "wx", 0o600);
    await handle.writeFile(`${JSON.stringify(records, null, 2)}\n`, "utf8");
    await handle.sync();
    await handle.close();
    handle = null;
    await fs.rename(temporary, AUTH_SESSIONS_FILE);
  } finally {
    if (handle) await handle.close().catch(() => {});
    await fs.rm(temporary, { force: true }).catch(() => {});
  }
}

function mutateAuthSessions(mutator) {
  const operation = authSessionOperation.then(async () => {
    const previous = authSessions;
    const next = new Map(previous);
    const result = mutator(next);
    await writeAuthSessions([...next.values()]);
    authSessions = next;
    return result;
  });
  authSessionOperation = operation.catch(() => {});
  return operation;
}

async function createAuthSession(user) {
  const now = Date.now();
  const session = { id: crypto.randomUUID(), userId: user.id, createdAt: now, expiresAt: now + AUTH_SESSION_TTL_MS };
  const evicted = await mutateAuthSessions((next) => {
    for (const [id, item] of next) if (item.expiresAt <= now) next.delete(id);
    const own = [...next.values()]
      .filter((item) => item.userId === user.id)
      .sort((left, right) => left.createdAt - right.createdAt);
    const removed = own.slice(0, Math.max(0, own.length - MAX_AUTH_SESSIONS_PER_USER + 1));
    for (const item of removed) next.delete(item.id);
    next.set(session.id, session);
    return removed.map((item) => item.id);
  });
  for (const id of evicted) closeTerminalConnectionsForSession(id);
  return session;
}

async function revokeAuthSession(id) {
  if (typeof id !== "string" || !authSessions.has(id)) return false;
  await mutateAuthSessions((next) => next.delete(id));
  closeTerminalConnectionsForSession(id);
  return true;
}

/* ---------- helpers ---------- */
function sendJson(res, status, body, extra = {}) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  for (const [k, v] of Object.entries(extra)) res.setHeader(k, v);
  res.end(JSON.stringify(body));
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let settled = false;
    req.on("data", (chunk) => {
      if (settled) return;
      size += chunk.length;
      if (size > JSON_BODY_MAX_BYTES) {
        settled = true;
        reject(httpError(413, `JSON request body exceeds ${JSON_BODY_MAX_BYTES} bytes`));
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      if (settled) return;
      try {
        settled = true;
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"));
      } catch {
        settled = true;
        reject(httpError(400, "request body must be valid JSON"));
      }
    });
    req.on("error", (error) => {
      if (!settled) {
        settled = true;
        reject(error);
      }
    });
  });
}
function cookieValues(req, name) {
  const values = [];
  for (const part of String(req.headers?.cookie || "").split(";")) {
    const separator = part.indexOf("=");
    if (separator < 0 || part.slice(0, separator).trim() !== name) continue;
    const raw = part.slice(separator + 1).trim();
    try { values.push(decodeURIComponent(raw)); }
    catch { values.push(raw); }
    if (values.length >= 128) break;
  }
  return values;
}

function getUser(req) {
  for (const token of cookieValues(req, "reaper_access")) {
    try {
      const user = jwt.verify(token, ACCESS_SECRET);
      const session = typeof user.jti === "string" ? authSessions.get(user.jti) : null;
      if (session && session.userId === user.sub && session.expiresAt > Date.now()) return user;
    } catch {}
  }
  return null;
}
function signAccess(user, authSession) {
  return jwt.sign(
    { sub: user.id, username: user.username, role: user.role },
    ACCESS_SECRET,
    { expiresIn: "8h", jwtid: authSession.id }
  );
}
function addCookie(res, value) {
  const existing = res.getHeader("Set-Cookie");
  if (!existing) res.setHeader("Set-Cookie", value);
  else if (Array.isArray(existing)) res.setHeader("Set-Cookie", [...existing, value]);
  else res.setHeader("Set-Cookie", [existing, value]);
}
function setAuthCookie(res, token) {
  addCookie(res, `reaper_access=${token}; ${COOKIE_BASE}; Max-Age=${60*60*8}; Priority=High`);
}

/* ---------- auth hardening: rate limit, CSRF, audit, ownership ---------- */
const AUDIT_LOG = path.join(STATE_DIR, "audit.log");
const AUDIT_MAX_BYTES = 10 * 1024 * 1024;
const MAX_RATE_LIMIT_KEYS = 4096;
const loginAttempts = new Map();
let loginAttemptsLastPrunedAt = 0;
let auditOperation = Promise.resolve();
const RATE_LIMIT_MAX = Number(process.env.AUTH_RATE_LIMIT_MAX || 10);
const RATE_LIMIT_WINDOW_MS = Number(process.env.AUTH_RATE_LIMIT_WINDOW_MS || 15 * 60 * 1000);
if (!Number.isSafeInteger(RATE_LIMIT_MAX) || RATE_LIMIT_MAX < 1) throw new Error("AUTH_RATE_LIMIT_MAX must be a positive integer");
if (!Number.isSafeInteger(RATE_LIMIT_WINDOW_MS) || RATE_LIMIT_WINDOW_MS < 1000) throw new Error("AUTH_RATE_LIMIT_WINDOW_MS must be an integer of at least 1000");
const MAX_ACTIVE_PASSWORD_CHECKS = 4;
const GLOBAL_PASSWORD_CHECKS_PER_MINUTE = 120;
let activePasswordChecks = 0;
let passwordCheckWindowAt = Date.now();
let passwordChecksInWindow = 0;
const CSRF_COOKIE = "reaper_csrf";

function clientIp(req) {
  return String(req.headers?.["x-forwarded-for"] || req.socket?.remoteAddress || "unknown")
    .split(",")[0]
    .trim()
    .slice(0, 128);
}

function pruneRateLimitStore(store, now, ttl, lastPrunedAt) {
  if (now - lastPrunedAt < Math.min(ttl, 60_000) && store.size < MAX_RATE_LIMIT_KEYS) return lastPrunedAt;
  for (const [key, value] of store) {
    if (now - value.lastSeenAt > ttl) store.delete(key);
  }
  while (store.size >= MAX_RATE_LIMIT_KEYS) store.delete(store.keys().next().value);
  return now;
}

function touchRateLimitEntry(store, key, value) {
  store.delete(key);
  store.set(key, value);
}

function checkRateLimit(req) {
  const ip = clientIp(req);
  const now = Date.now();
  loginAttemptsLastPrunedAt = pruneRateLimitStore(loginAttempts, now, RATE_LIMIT_WINDOW_MS, loginAttemptsLastPrunedAt);
  const rec = loginAttempts.get(ip);
  if (!rec || now - rec.firstAt > RATE_LIMIT_WINDOW_MS) {
    loginAttempts.delete(ip);
    return { ok: true, remaining: RATE_LIMIT_MAX };
  }
  touchRateLimitEntry(loginAttempts, ip, { ...rec, lastSeenAt: now });
  if (rec.count >= RATE_LIMIT_MAX) return { ok: false, retryAfter: Math.ceil((rec.firstAt + RATE_LIMIT_WINDOW_MS - now) / 1000) };
  return { ok: true, remaining: RATE_LIMIT_MAX - rec.count };
}

function recordLoginFailure(req) {
  const ip = clientIp(req);
  const now = Date.now();
  loginAttemptsLastPrunedAt = pruneRateLimitStore(loginAttempts, now, RATE_LIMIT_WINDOW_MS, loginAttemptsLastPrunedAt);
  const current = loginAttempts.get(ip);
  const rec = !current || now - current.firstAt > RATE_LIMIT_WINDOW_MS
    ? { count: 0, firstAt: now, lastSeenAt: now }
    : current;
  rec.count += 1;
  rec.lastSeenAt = now;
  touchRateLimitEntry(loginAttempts, ip, rec);
}

function clearLoginFailures(req) {
  loginAttempts.delete(clientIp(req));
}
function reservePasswordCheck() {
  const now = Date.now();
  if (now - passwordCheckWindowAt >= 60_000) {
    passwordCheckWindowAt = now;
    passwordChecksInWindow = 0;
  }
  if (
    activePasswordChecks >= MAX_ACTIVE_PASSWORD_CHECKS ||
    passwordChecksInWindow >= GLOBAL_PASSWORD_CHECKS_PER_MINUTE
  ) return null;
  activePasswordChecks += 1;
  passwordChecksInWindow += 1;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    activePasswordChecks = Math.max(0, activePasswordChecks - 1);
  };
}

async function appendAuditLine(line) {
  const pending = auditOperation.catch(() => {}).then(async () => {
    const stat = await fs.stat(AUDIT_LOG).catch((error) => error.code === "ENOENT" ? null : Promise.reject(error));
    if (stat && stat.size + Buffer.byteLength(line) > AUDIT_MAX_BYTES) {
      await fs.rm(`${AUDIT_LOG}.1`, { force: true });
      await fs.rename(AUDIT_LOG, `${AUDIT_LOG}.1`);
    }
    await fs.appendFile(AUDIT_LOG, line, { mode: 0o600 });
  });
  auditOperation = pending.catch(() => {});
  await pending;
}

async function audit(action, req, detail = {}) {
  const base = {
    ts: new Date().toISOString(),
    user: String(req?.user?.username || "anonymous").slice(0, 64),
    ip: clientIp(req || {}),
    action: String(action).slice(0, 64)
  };
  let record = { ...base, ...detail };
  let line = `${JSON.stringify(record)}\n`;
  if (Buffer.byteLength(line) > 8192) {
    record = { ...base, detailTruncated: true };
    line = `${JSON.stringify(record)}\n`;
  }
  try { await appendAuditLine(line); } catch {}
  state.audit = state.audit || [];
  state.audit.unshift(record);
  if (state.audit.length > 200) state.audit.length = 200;
}
function getCsrfTokens(req) { return cookieValues(req, CSRF_COOKIE); }
function getCsrfToken(req) { return getCsrfTokens(req).at(-1) || null; }
function generateCsrfToken() { return crypto.randomBytes(24).toString("hex"); }
function setCsrfCookieImpl(res, token) {
  addCookie(res, `${CSRF_COOKIE}=${token}; ${COOKIE_BASE}; Max-Age=${60*60*8}; Priority=High`);
}
function clearAuthCookie(res) {
  addCookie(res, `reaper_access=; ${COOKIE_BASE}; Max-Age=0`);
  addCookie(res, `${CSRF_COOKIE}=; ${COOKIE_BASE}; Max-Age=0`);
}
function checkCsrf(req) {
  if (req.method === "GET" || req.method === "HEAD" || req.method === "OPTIONS") return true;
  const headerTok = req.headers["x-csrf-token"];
  return typeof headerTok === "string" && getCsrfTokens(req).some((token) => constantTimeEqual(token, headerTok));
}
function canAccessProject(name) {
  // single-user V1: any authenticated user; hook for future per-user ACL
  const p = projectDir(name);
  return fss.existsSync(p);
}
function assertProjectAccess(name) {
  if (!canAccessProject(name)) {
    const e = new Error("project not found");
    e.statusCode = 404;
    throw e;
  }
}
const ALLOWED_ORIGINS = (process.env.CORS_ORIGINS || "http://localhost:5174,http://127.0.0.1:5174,http://localhost:5173,http://127.0.0.1:5173").split(",").map((s) => s.trim()).filter(Boolean);
function setCors(res, origin) {
  if (ALLOWED_ORIGINS.includes("*") || ALLOWED_ORIGINS.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin || "*");
    res.setHeader("Access-Control-Allow-Credentials", "true");
  }
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-CSRF-Token");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS");
}

function firstForwardedHeader(value) {
  return typeof value === "string" ? value.split(",")[0].trim() : "";
}

function normalizedOrigin(value) {
  try { return new URL(value).origin; } catch { return null; }
}

function isAllowedSocketOrigin(req) {
  const origin = normalizedOrigin(req.headers.origin);
  if (!origin) return false;
  const host = firstForwardedHeader(req.headers["x-forwarded-host"]) || firstForwardedHeader(req.headers.host);
  const protocol = firstForwardedHeader(req.headers["x-forwarded-proto"]) || (req.socket?.encrypted ? "https" : "http");
  const sameOrigin = host ? normalizedOrigin(`${protocol}://${host}`) : null;
  if (sameOrigin && origin === sameOrigin) return true;
  return ALLOWED_ORIGINS.some((allowed) => allowed === "*" || normalizedOrigin(allowed) === origin);
}

function constantTimeEqual(left, right) {
  if (typeof left !== "string" || typeof right !== "string") return false;
  const a = Buffer.from(left);
  const b = Buffer.from(right);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

/* ---------- fs safety ---------- */
function httpError(statusCode, message, detail = {}) {
  const error = new Error(message);
  error.statusCode = statusCode;
  error.detail = detail;
  return error;
}

function isPathInside(root, target) {
  const a = path.resolve(root);
  const b = path.resolve(target);
  return b === a || b.startsWith(a + path.sep);
}

function isDirectChildName(name) {
  return typeof name === "string"
    && name.length > 0
    && name !== "."
    && name !== ".."
    && !name.includes("/")
    && !name.includes("\\")
    && !name.includes("\0");
}

function directChildPath(root, name) {
  if (!isDirectChildName(name)) throw httpError(400, "invalid resource name");
  const base = path.resolve(root);
  const target = path.resolve(base, name);
  if (path.dirname(target) !== base) throw httpError(400, "invalid resource name");
  return target;
}

function safeJoin(root, rel, { allowRoot = false } = {}) {
  if (typeof rel !== "string" || !rel || rel.includes("\0") || path.isAbsolute(rel)) return null;
  const base = path.resolve(root);
  const target = path.resolve(base, rel);
  if (!isPathInside(base, target) || (!allowRoot && target === base)) return null;
  return target;
}

/* ---------- schemas ---------- */
const loginSchema = z.object({
  username: z.string().min(3).max(64),
  password: z.string().min(1).max(256)
});
const writeFileSchema = z.object({
  path: z.string().min(1).max(512),
  content: z.string().max(256 * 1024 * 1024)
});
const mkdirSchema = z.object({ path: z.string().min(1) });
const deleteSchema = z.object({ path: z.string().min(1) });
const createProjectSchema = z.object({
  name: z.string().regex(PROJECT_NAME_RE),
  mode: z.enum(["persistent", "temporary"]).optional()
});

/* ---------- fs ops ---------- */
const NOFOLLOW = fss.constants.O_NOFOLLOW || 0;
const SAFE_READ_FLAGS = fss.constants.O_RDONLY | NOFOLLOW;
const SAFE_TEMP_FLAGS = fss.constants.O_WRONLY | fss.constants.O_CREAT | fss.constants.O_EXCL | NOFOLLOW;
const SAFE_DIRECTORY_FLAGS = fss.constants.O_RDONLY | (fss.constants.O_DIRECTORY || 0) | NOFOLLOW;

function fileErrorResult(error, fallbackStatus = 400) {
  const status = error.statusCode || (error.code === "ENOENT" ? 404 : fallbackStatus);
  return { status, body: { error: error.message, ...(error.detail || {}) } };
}

async function safeRootMetadata(root) {
  const rootPath = path.resolve(root);
  let stat;
  try { stat = await fs.lstat(rootPath); }
  catch (error) {
    if (error.code === "ENOENT") throw httpError(404, "resource not found");
    throw error;
  }
  if (stat.isSymbolicLink()) throw httpError(400, "symlink root blocked");
  if (!stat.isDirectory()) throw httpError(400, "resource root is not a directory");
  const rootRealPath = await fs.realpath(rootPath);
  return { rootPath, rootRealPath };
}

async function ensureSafeDirectory(root, directory, { create = false } = {}) {
  const rootMeta = await safeRootMetadata(root);
  const target = path.resolve(directory);
  if (!isPathInside(rootMeta.rootPath, target)) throw httpError(400, "path traversal blocked");
  const relative = path.relative(rootMeta.rootPath, target);
  let current = rootMeta.rootPath;
  for (const segment of relative ? relative.split(path.sep) : []) {
    current = path.join(current, segment);
    let stat;
    try { stat = await fs.lstat(current); }
    catch (error) {
      if (error.code !== "ENOENT" || !create) throw error;
      await fs.mkdir(current);
      stat = await fs.lstat(current);
    }
    if (stat.isSymbolicLink()) throw httpError(400, "symlink traversal blocked");
    if (!stat.isDirectory()) throw httpError(400, "path component is not a directory");
  }
  const directoryRealPath = await fs.realpath(current);
  if (!isPathInside(rootMeta.rootRealPath, directoryRealPath)) throw httpError(400, "symlink traversal blocked");
  return { ...rootMeta, directory: current, directoryRealPath };
}

async function openPinnedDirectory(root, directory, { create = false } = {}) {
  const rootMeta = await safeRootMetadata(root);
  const target = path.resolve(directory);
  if (!isPathInside(rootMeta.rootPath, target)) throw httpError(400, "path traversal blocked");
  const segments = path.relative(rootMeta.rootPath, target).split(path.sep).filter(Boolean);
  let currentPath = rootMeta.rootPath;
  let currentHandle = await fs.open(rootMeta.rootPath, SAFE_DIRECTORY_FLAGS);
  try {
    await verifyOpenedFile(currentHandle, rootMeta.rootPath, rootMeta.rootRealPath);
    for (const segment of segments) {
      const descriptorChild = `/proc/self/fd/${currentHandle.fd}/${segment}`;
      let stat;
      try { stat = await fs.lstat(descriptorChild); }
      catch (error) {
        if (error.code !== "ENOENT" || !create) throw error;
        await fs.mkdir(descriptorChild);
        stat = await fs.lstat(descriptorChild);
      }
      if (stat.isSymbolicLink()) throw httpError(400, "symlink traversal blocked");
      if (!stat.isDirectory()) throw httpError(400, "path component is not a directory");
      const nextHandle = await fs.open(descriptorChild, SAFE_DIRECTORY_FLAGS);
      const nextStat = await nextHandle.stat();
      if (nextStat.dev !== stat.dev || nextStat.ino !== stat.ino) {
        await nextHandle.close().catch(() => {});
        throw httpError(409, "directory changed while opening");
      }
      const nextRealPath = await fs.realpath(`/proc/self/fd/${nextHandle.fd}`);
      if (!isPathInside(rootMeta.rootRealPath, nextRealPath)) {
        await nextHandle.close().catch(() => {});
        throw httpError(400, "symlink traversal blocked");
      }
      await currentHandle.close();
      currentHandle = nextHandle;
      currentPath = path.join(currentPath, segment);
    }
    return {
      ...rootMeta,
      directory: currentPath,
      directoryRealPath: await fs.realpath(`/proc/self/fd/${currentHandle.fd}`),
      handle: currentHandle
    };
  } catch (error) {
    await currentHandle.close().catch(() => {});
    throw error;
  }
}

async function resolveSafeExistingPath(root, rel, expected = null) {
  const target = safeJoin(root, rel);
  if (!target) throw httpError(400, "path traversal blocked");
  const parent = await ensureSafeDirectory(root, path.dirname(target));
  const stat = await fs.lstat(target);
  if (stat.isSymbolicLink()) throw httpError(400, "symlink traversal blocked");
  if (expected === "file" && !stat.isFile()) throw httpError(400, "not a file");
  if (expected === "directory" && !stat.isDirectory()) throw httpError(400, "not a directory");
  const targetRealPath = await fs.realpath(target);
  if (!isPathInside(parent.rootRealPath, targetRealPath)) throw httpError(400, "symlink traversal blocked");
  return { ...parent, target, targetRealPath, stat };
}

async function verifyOpenedFile(handle, target, rootRealPath) {
  const openedStat = await handle.stat();
  const pathStat = await fs.stat(target);
  if (openedStat.dev !== pathStat.dev || openedStat.ino !== pathStat.ino) {
    throw httpError(409, "file changed while opening");
  }
  const targetRealPath = await fs.realpath(target);
  if (!isPathInside(rootRealPath, targetRealPath)) throw httpError(400, "symlink traversal blocked");
  if (process.platform === "linux") {
    const descriptorPath = await fs.realpath(`/proc/self/fd/${handle.fd}`);
    if (!isPathInside(rootRealPath, descriptorPath)) throw httpError(400, "symlink traversal blocked");
  }
  return openedStat;
}

async function openSafeReadFile(root, rel) {
  const resolved = await resolveSafeExistingPath(root, rel, "file");
  const handle = await fs.open(resolved.target, SAFE_READ_FLAGS);
  try {
    const stat = await verifyOpenedFile(handle, resolved.target, resolved.rootRealPath);
    return { handle, stat, target: resolved.target, rootRealPath: resolved.rootRealPath };
  } catch (error) {
    await handle.close().catch(() => {});
    throw error;
  }
}

async function openSafeTemporaryFile(root, destination, marker) {
  const parent = process.platform === "linux"
    ? await openPinnedDirectory(root, path.dirname(destination), { create: true })
    : await ensureSafeDirectory(root, path.dirname(destination), { create: true });
  const name = `.${path.basename(destination)}.${marker}-${crypto.randomBytes(8).toString("hex")}`;
  const temporary = path.join(parent.directory, name);
  const openedPath = parent.handle ? `/proc/self/fd/${parent.handle.fd}/${name}` : temporary;
  let handle;
  try {
    handle = await fs.open(openedPath, SAFE_TEMP_FLAGS, 0o600);
    await verifyOpenedFile(handle, openedPath, parent.rootRealPath);
    return { handle, temporary, name, parentHandle: parent.handle || null };
  } catch (error) {
    if (handle) await handle.close().catch(() => {});
    await fs.rm(openedPath, { force: true }).catch(() => {});
    if (parent.handle) await parent.handle.close().catch(() => {});
    throw error;
  }
}

function temporaryDescriptorPath(temporary) {
  return temporary.parentHandle
    ? `/proc/self/fd/${temporary.parentHandle.fd}/${temporary.name}`
    : temporary.temporary;
}

async function discardTemporaryFile(temporary) {
  await fs.rm(temporaryDescriptorPath(temporary), { force: true }).catch(() => {});
  if (temporary.parentHandle) {
    await temporary.parentHandle.close().catch(() => {});
    temporary.parentHandle = null;
  }
}

async function promoteTemporaryFile(root, temporary, destination) {
  if (temporary.parentHandle) {
    if (path.dirname(temporary.temporary) !== path.dirname(destination)) {
      throw httpError(400, "temporary file parent mismatch");
    }
    const parentRealPath = await fs.realpath(`/proc/self/fd/${temporary.parentHandle.fd}`);
    const rootRealPath = await fs.realpath(path.resolve(root));
    if (!isPathInside(rootRealPath, parentRealPath)) throw httpError(400, "symlink traversal blocked");
    const source = temporaryDescriptorPath(temporary);
    const target = `/proc/self/fd/${temporary.parentHandle.fd}/${path.basename(destination)}`;
    try {
      const destinationStat = await fs.lstat(target);
      if (destinationStat.isSymbolicLink()) throw httpError(400, "symlink destination blocked");
      if (destinationStat.isDirectory()) throw httpError(400, "destination is a directory");
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    await fs.rename(source, target);
    await temporary.parentHandle.close();
    temporary.parentHandle = null;
    return;
  }
  await ensureSafeDirectory(root, path.dirname(destination));
  const temporaryRel = path.relative(path.resolve(root), temporary.temporary);
  await resolveSafeExistingPath(root, temporaryRel, "file");
  try {
    const destinationStat = await fs.lstat(destination);
    if (destinationStat.isSymbolicLink()) throw httpError(400, "symlink destination blocked");
    if (destinationStat.isDirectory()) throw httpError(400, "destination is a directory");
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  await fs.rename(temporary.temporary, destination);
}

async function listProjects() {
  try {
    const items = await fs.readdir(VPS_PROJECTS, { withFileTypes: true });
    return items.filter((d) => d.isDirectory() && !d.name.startsWith(".")).map((d) => d.name).sort();
  } catch { return []; }
}


function decodeUtf8Text(data, size) {
  if (data.includes(0)) throw httpError(415, "binary file", { binary: true, size });
  try { return new TextDecoder("utf-8", { fatal: true }).decode(data); }
  catch { throw httpError(415, "binary file", { binary: true, size }); }
}
const FILE_PREVIEW_MAX_BYTES = 8 * 1024 * 1024;
const MAX_ACTIVE_PREVIEW_BYTES = 32 * 1024 * 1024;
const DIRECTORY_PAGE_SIZE = 1000;
let activePreviewBytes = 0;

async function readFile(rootDir, rel) {
  const opened = await openSafeReadFile(rootDir, rel);
  let reserved = false;
  try {
    if (opened.stat.size > FILE_PREVIEW_MAX_BYTES) {
      throw httpError(413, "file is too large for text preview", {
        code: "FILE_PREVIEW_TOO_LARGE",
        size: opened.stat.size,
        maxBytes: FILE_PREVIEW_MAX_BYTES,
        download: true
      });
    }
    if (activePreviewBytes + opened.stat.size > MAX_ACTIVE_PREVIEW_BYTES) {
      throw httpError(429, "text preview capacity is temporarily busy", {
        code: "FILE_PREVIEW_BUSY",
        retryable: true
      });
    }
    activePreviewBytes += opened.stat.size;
    reserved = true;
    const data = await opened.handle.readFile();
    const content = decodeUtf8Text(data, opened.stat.size);
    return { content, bytes: opened.stat.size, updatedAt: opened.stat.mtime.toISOString() };
  } finally {
    if (reserved) activePreviewBytes -= opened.stat.size;
    await opened.handle.close().catch(() => {});
  }
}

async function writeFile(rootDir, rel, content) {
  const destination = safeJoin(rootDir, rel);
  if (!destination) throw httpError(400, "path traversal blocked");
  const temporary = await openSafeTemporaryFile(rootDir, destination, "write");
  let closed = false;
  try {
    await temporary.handle.writeFile(content, "utf8");
    await temporary.handle.sync();
    const stat = await temporary.handle.stat();
    await temporary.handle.close();
    closed = true;
    await promoteTemporaryFile(rootDir, temporary, destination);
    return { bytes: stat.size, updatedAt: stat.mtime.toISOString() };
  } catch (error) {
    if (!closed) await temporary.handle.close().catch(() => {});
    await discardTemporaryFile(temporary);
    throw error;
  }
}

async function deleteFile(rootDir, rel) {
  const target = safeJoin(rootDir, rel);
  if (!target) throw httpError(400, "path traversal blocked");
  const parent = await ensureSafeDirectory(rootDir, path.dirname(target));
  const original = await fs.lstat(target);
  if (process.platform !== "linux") {
    await ensureSafeDirectory(rootDir, path.dirname(target));
    await fs.rm(target, { recursive: true, force: true });
    return { ok: true };
  }
  const parentHandle = await fs.open(parent.directory, SAFE_DIRECTORY_FLAGS);
  try {
    await verifyOpenedFile(parentHandle, parent.directory, parent.rootRealPath);
    const descriptorTarget = `/proc/self/fd/${parentHandle.fd}/${path.basename(target)}`;
    const current = await fs.lstat(descriptorTarget);
    if (current.dev !== original.dev || current.ino !== original.ino) {
      throw httpError(409, "file changed before deletion");
    }
    await fs.rm(descriptorTarget, { recursive: true, force: true });
  } finally {
    await parentHandle.close().catch(() => {});
  }
  return { ok: true };
}

async function mkdir(rootDir, rel) {
  const target = safeJoin(rootDir, rel);
  if (!target) throw httpError(400, "path traversal blocked");
  if (process.platform === "linux") {
    const opened = await openPinnedDirectory(rootDir, target, { create: true });
    await opened.handle.close();
  } else {
    await ensureSafeDirectory(rootDir, target, { create: true });
  }
  return { ok: true };
}

/* ---------- trusted per-project configuration ---------- */
function projectDir(name) { return directChildPath(VPS_PROJECTS, name); }
function projectControlDir(name) { return directChildPath(path.join(STATE_DIR, "projects"), name); }

function normalizeProjectConfig(name, value = {}) {
  const createdAt = typeof value.createdAt === "string" && Number.isFinite(Date.parse(value.createdAt))
    ? new Date(value.createdAt).toISOString()
    : null;
  const owner = typeof value.owner === "string" && /^[A-Za-z0-9_.-]{1,64}$/.test(value.owner)
    ? value.owner
    : null;
  return {
    name,
    mode: value.mode === "temporary" ? "temporary" : "persistent",
    createdAt,
    owner
  };
}

async function writeProjectConfig(name, value) {
  const directory = projectControlDir(name);
  await fs.mkdir(directory, { recursive: true, mode: 0o700 });
  const destination = path.join(directory, "config.json");
  const temporary = path.join(directory, `.config-${crypto.randomBytes(8).toString("hex")}.tmp`);
  let handle;
  try {
    handle = await fs.open(temporary, "wx", 0o600);
    await handle.writeFile(`${JSON.stringify(normalizeProjectConfig(name, value), null, 2)}\n`, "utf8");
    await handle.sync();
    await handle.close();
    handle = null;
    await fs.rename(temporary, destination);
  } finally {
    if (handle) await handle.close().catch(() => {});
    await fs.rm(temporary, { force: true }).catch(() => {});
  }
}

async function readLegacyProjectConfig(name) {
  let opened;
  try {
    opened = await openSafeReadFile(projectDir(name), ".reaper/config.json");
    if (opened.stat.size > 64 * 1024) return null;
    return JSON.parse(await opened.handle.readFile("utf8"));
  } catch {
    return null;
  } finally {
    if (opened?.handle) await opened.handle.close().catch(() => {});
  }
}

async function readProjectConfig(name) {
  const destination = path.join(projectControlDir(name), "config.json");
  try {
    return normalizeProjectConfig(name, JSON.parse(await fs.readFile(destination, "utf8")));
  } catch (error) {
    if (error.code !== "ENOENT") throw new Error(`trusted project configuration is invalid: ${error.message}`);
  }
  const config = normalizeProjectConfig(name, await readLegacyProjectConfig(name) || {});
  await writeProjectConfig(name, config);
  return config;
}

/* ---------- auth (hardened) ---------- */
const auth = {
  login: async (req, body) => {
    const limit = checkRateLimit(req);
    if (!limit.ok) return { status: 429, body: { error: "too many attempts", retryAfter: limit.retryAfter } };
    let parsed;
    try { parsed = loginSchema.parse(body); }
    catch (e) { recordLoginFailure(req); return { status: 400, body: { error: e.issues?.[0]?.message || "invalid input" } }; }
    const { username, password } = parsed;
    const releasePasswordCheck = reservePasswordCheck();
    if (!releasePasswordCheck) return { status: 429, body: { error: "authentication capacity is busy", retryAfter: 1 } };
    const user = state.users.find((u) => u.username === username);
    const comparisonHash = user?.password_hash || state.users[0]?.password_hash;
    let passwordMatches = false;
    try {
      if (comparisonHash) passwordMatches = await bcrypt.compare(password, comparisonHash);
    } finally {
      releasePasswordCheck();
    }
    if (!user || !passwordMatches) {
      recordLoginFailure(req);
      await audit("auth.login.fail", req, { username });
      return { status: 401, body: { error: "Invalid credentials" } };
    }
    clearLoginFailures(req);
    const authSession = await createAuthSession(user);
    const token = signAccess(user, authSession);
    const csrf = generateCsrfToken();
    await audit("auth.login.ok", req, { username });
    return {
      status: 200,
      body: { user: { id: user.id, username: user.username, role: user.role }, csrfToken: csrf },
      cookies: { access: token, csrf }
    };
  },
  me: async (req) => {
    const u = getUser(req);
    if (!u) return { status: 401, body: { error: "not authenticated" } };
    return { status: 200, body: { user: { id: u.sub, username: u.username, role: u.role } } };
  },
  logout: async (req) => {
    if (req.apiToken || typeof req.user?.jti !== "string") {
      return { status: 400, body: { error: "logout requires a browser session" } };
    }
    await revokeAuthSession(req.user.jti);
    await audit("auth.logout", req);
    return { status: 200, body: { ok: true }, cookies: { clear: true } };
  },
  csrf: async (req) => {
    const u = getUser(req);
    if (!u) return { status: 401, body: { error: "not authenticated" } };
    const existing = getCsrfToken(req);
    const csrf = existing || generateCsrfToken();
    return {
      status: 200,
      body: { csrfToken: csrf },
      ...(existing ? {} : { cookies: { csrf } })
    };
  }
};

/* ---------- projects ---------- */
const projects = {
  list: async (req) => {
    await audit("projects.list", req);
    return { status: 200, body: { projects: await listProjects() } };
  },
  get: async (req, body, params) => {
    assertProjectAccess(params.name);
    const config = await readProjectConfig(params.name);
    return { status: 200, body: { project: config } };
  },
  create: async (req, body) => {
    const { name, mode } = createProjectSchema.parse(body);
    const p = projectDir(name);
    if (fss.existsSync(p)) return { status: 409, body: { error: "project exists" } };
    deleteProjectTokenStore(name);
    await fs.mkdir(p, { recursive: false });
    const config = { name, mode: mode || "persistent", createdAt: new Date().toISOString(), owner: req.user.username };
    try {
      await resetProjectState(name);
      await writeProjectConfig(name, config);
      await openProjectShell({ path: name, sessionName: "main", title: "main", bashrc: "# project shell config\n" });
    } catch (error) {
      try {
        await destroyPod(name);
        await resetProjectState(name);
        await fs.rm(p, { recursive: true, force: true });
      } catch (cleanupError) {
        throw new AggregateError([error, cleanupError], `project ${name} creation failed and rollback was incomplete`);
      }
      throw error;
    }
    await audit("projects.create", req, { name });
    return { status: 201, body: { project: config } };
  },
  remove: async (req, body, params) => {
    assertProjectAccess(params.name);
    const result = await destroyProjectRuntime(params.name);
    deleteProjectTokenStore(params.name);
    await audit("projects.delete", req, { name: params.name, routeCleanupPending: result.routeCleanupPending });
    return { status: 200, body: { ok: true, routeCleanupPending: result.routeCleanupPending } };
  }
};

/* ---------- project files ---------- */
const UPLOAD_MAX_BYTES = Number(process.env.UPLOAD_MAX_BYTES) || 2 * 1024 * 1024 * 1024; // 2 GB

// Shallow, lazy directory listing. Real repos hold node_modules/.git with
// hundreds of thousands of files; a recursive walk would never return, so the
// client loads one directory level at a time and expands on demand.
async function listDir(root, rel, cursor = "") {
  const resolved = rel
    ? await resolveSafeExistingPath(root, rel, "directory")
    : await safeRootMetadata(root);
  const dir = rel ? resolved.target : resolved.rootPath;
  const rawCursor = cursor === undefined || cursor === null || cursor === "" ? "0" : String(cursor);
  if (!/^(?:0|[1-9]\d{0,9})$/.test(rawCursor)) throw httpError(400, "invalid directory cursor");
  const offset = Number(rawCursor);
  const out = [];
  let position = 0;
  let nextCursor = null;
  const handle = await fs.opendir(dir);
  for await (const ent of handle) {
    const index = position++;
    if (index < offset) continue;
    const child = path.join(dir, ent.name);
    const childRel = path.relative(root, child).split(path.sep).join("/");
    let item = null;
    if (ent.isSymbolicLink()) {
      item = { type: "file", name: ent.name, path: childRel, symlink: true };
    } else if (ent.isDirectory()) {
      item = { type: "directory", name: ent.name, path: childRel };
    } else if (ent.isFile()) {
      let size = null;
      let mtime = null;
      try {
        const stat = await fs.lstat(child);
        if (stat.isFile()) { size = stat.size; mtime = stat.mtimeMs; }
      } catch {}
      item = { type: "file", name: ent.name, path: childRel, size, mtime };
    }
    if (!item) continue;
    if (out.length >= DIRECTORY_PAGE_SIZE) {
      nextCursor = String(index);
      break;
    }
    out.push(item);
  }
  await ensureSafeDirectory(root, dir);
  out.sort((a, b) => (a.type !== b.type ? (a.type === "directory" ? -1 : 1) : a.name.localeCompare(b.name)));
  return { entries: out, nextCursor };
}

function makeFileApi(rootFor, missing) {
  return {
    tree: async (req, body, params, qs) => {
      const root = rootFor(params);
      if (!fss.existsSync(root)) return { status: 404, body: { error: missing } };
      try {
        const page = await listDir(root, qs.path || "", qs.cursor || "");
        return { status: 200, body: { path: qs.path || "", ...page } };
      }
      catch (error) { return fileErrorResult(error); }
    },
    read: async (req, body, params, qs) => {
      const root = rootFor(params);
      if (!fss.existsSync(root)) return { status: 404, body: { error: missing } };
      try { return { status: 200, body: await readFile(root, qs.path) }; }
      catch (error) { return fileErrorResult(error); }
    },
    write: async (req, body, params) => {
      const root = rootFor(params);
      if (!fss.existsSync(root)) return { status: 404, body: { error: missing } };
      const { path: rel, content } = writeFileSchema.parse(body);
      try { return { status: 200, body: await writeFile(root, rel, content) }; }
      catch (error) { return fileErrorResult(error); }
    },
    delete: async (req, body, params) => {
      const root = rootFor(params);
      if (!fss.existsSync(root)) return { status: 404, body: { error: missing } };
      const { path: rel } = deleteSchema.parse(body);
      try { return { status: 200, body: await deleteFile(root, rel) }; }
      catch (error) { return fileErrorResult(error); }
    },
    mkdir: async (req, body, params) => {
      const root = rootFor(params);
      if (!fss.existsSync(root)) return { status: 404, body: { error: missing } };
      const { path: rel } = mkdirSchema.parse(body);
      try { return { status: 200, body: await mkdir(root, rel) }; }
      catch (error) { return fileErrorResult(error); }
    },
    upload: async (req, res, params, qs) => {
      const root = rootFor(params);
      if (!fss.existsSync(root)) return sendJson(res, 404, { error: missing });
      const destination = safeJoin(root, qs.path);
      if (!destination) return sendJson(res, 400, { error: "path traversal blocked" });
      let temporary = null;
      let closed = false;
      try {
        temporary = await openSafeTemporaryFile(root, destination, "upload");
        const declaredLength = Number(req.headers["content-length"]);
        let tooLarge = Number.isFinite(declaredLength) && declaredLength > UPLOAD_MAX_BYTES;
        let bytes = 0;
        for await (const chunk of req) {
          bytes += chunk.length;
          if (tooLarge || bytes > UPLOAD_MAX_BYTES) {
            tooLarge = true;
            continue;
          }
          let offset = 0;
          while (offset < chunk.length) {
            const written = await temporary.handle.write(chunk, offset, chunk.length - offset, null);
            if (written.bytesWritten < 1) throw new Error("upload write made no progress");
            offset += written.bytesWritten;
          }
        }
        if (!req.complete || req.aborted) throw httpError(400, "upload incomplete");
        if (tooLarge) {
          await temporary.handle.close();
          closed = true;
          await discardTemporaryFile(temporary);
          return sendJson(res, 413, { error: "file exceeds upload limit" });
        }
        await temporary.handle.sync();
        await temporary.handle.close();
        closed = true;
        await promoteTemporaryFile(root, temporary, destination);
        return sendJson(res, 200, { ok: true, path: qs.path, bytes });
      } catch (error) {
        if (temporary && !closed) await temporary.handle.close().catch(() => {});
        if (temporary) await discardTemporaryFile(temporary);
        if (req.aborted || res.destroyed) return;
        const result = fileErrorResult(error, 500);
        return sendJson(res, result.status, result.body);
      }
    },
    download: async (req, res, params, qs) => {
      const root = rootFor(params);
      if (!fss.existsSync(root)) return sendJson(res, 404, { error: missing });
      let opened;
      try {
        opened = await openSafeReadFile(root, qs.path);
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/octet-stream");
        res.setHeader("Content-Length", opened.stat.size);
        res.setHeader("Content-Disposition", `attachment; filename="${path.basename(opened.target).replace(/["\r\n]/g, "")}"`);
        if (req.method === "HEAD") {
          await opened.handle.close();
          return res.end();
        }
        if (opened.stat.size === 0) {
          await opened.handle.close();
          return res.end();
        }
        const stream = opened.handle.createReadStream({
          autoClose: true,
          start: 0,
          end: opened.stat.size - 1
        });
        const abortStream = () => { if (!stream.destroyed) stream.destroy(); };
        res.once("close", abortStream);
        stream.once("close", () => res.off("close", abortStream));
        stream.on("error", () => { if (!res.destroyed) res.destroy(); });
        return stream.pipe(res);
      } catch (error) {
        if (opened?.handle) await opened.handle.close().catch(() => {});
        const result = fileErrorResult(error, 500);
        return sendJson(res, result.status, result.body);
      }
    }
  };
}

const files = makeFileApi((params) => projectDir(params.name), "project not found");

/* ---------- env + bashrc ---------- */
const env = {
  get: async (req, body, params) => {
    if (!fss.existsSync(projectDir(params.name))) return { status: 404, body: { error: "project not found" } };
    return { status: 200, body: { env: await getProjectEnv(params.name) } };
  },
  set: async (req, body, params) => {
    const root = projectDir(params.name);
    if (!fss.existsSync(root)) return { status: 404, body: { error: "project not found" } };
    if (!body.env || typeof body.env !== "object" || Array.isArray(body.env)) {
      return { status: 400, body: { error: "env must be an object" } };
    }
    const entries = Object.entries(body.env);
    if (entries.length > 256 || entries.some(([key, value]) =>
      key.length > 128 || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(key) ||
      typeof value !== "string" || value.includes("\0") || Buffer.byteLength(value) > 65_536)) {
      return { status: 400, body: { error: "env contains an invalid variable name or value" } };
    }
    const applied = await setProjectEnv(params.name, body.env);
    const currentSessions = await listSessions({ path: params.name });
    const restartRequired = currentSessions.some((session) => session.state === "running");
    // Existing shells receive the environment through the pod tmux server.
    return { status: 200, body: { ok: true, count: applied.count, sessions: applied.sessions, restartRequired, appliesTo: "new-processes" } };
  },
  getBashrc: async (req, body, params) => {
    if (!fss.existsSync(projectDir(params.name))) return { status: 404, body: { error: "project not found" } };
    return { status: 200, body: { content: await getProjectBashrc(params.name) } };
  },
  setBashrc: async (req, body, params) => {
    if (!fss.existsSync(projectDir(params.name))) return { status: 404, body: { error: "project not found" } };
    await setProjectBashrc(params.name, String(body.content || ""));
    return { status: 200, body: { ok: true } };
  }
};

/* ---------- sessions (delegated to local-shell.js) ---------- */
const sessions = {
  list: async (req, body, params) => {
    const list = await listSessions({ path: params.name });
    return { status: 200, body: { sessions: list } };
  },
  create: async (req, body, params) => {
    const result = await openProjectShell({
      path: params.name,
      sessionName: body?.name,
      title: body?.title
    });
    return { status: 201, body: { session: result.session } };
  },
  rename: async (req, body, params) => {
    const result = await renameSession(params.name, params.s, body?.title);
    return { status: result.ok ? 200 : 404, body: result };
  },
  destroy: async (req, body, params) => {
    const result = await destroySession(params.s, { project: params.name });
    return { status: result?.ok === false ? 404 : 200, body: result };
  }
};

const ports = {
  get: async (req, body, params) => ({ status: 200, body: await getProjectPorts(params.name) }),
  put: async (req, body, params) => ({ status: 200, body: await updateProjectPorts(params.name, body?.ports) })
};

/* ---------- project session log archive ---------- */
function resolveArchivedLog(scope, file) {
  if (!file || !/^[A-Za-z0-9._-]+\.log$/.test(file)) return null;
  const safe = String(scope || "").replace(/[^a-zA-Z0-9_.-]/g, "_").slice(0, 128) || "default";
  const folder = "projects";
  const full = path.join(SESSION_ARCHIVE_PATH, folder, safe, file);
  const normalized = path.normalize(full);
  if (!normalized.startsWith(path.normalize(path.join(SESSION_ARCHIVE_PATH, folder, safe)) + path.sep)) {
    return null;
  }
  return normalized;
}

const projectsSessionLogs = {
  list: async (req, body, params) => {
    const logs = await listArchivedSessionLogs(params.name);
    return { status: 200, body: { logs } };
  },
  read: async (req, body, params) => {
    const file = resolveArchivedLog(params.name, params.file);
    if (!file) return { status: 400, body: { error: "invalid log file name" } };
    try {
      const stat = await fs.stat(file);
      const data = await fs.readFile(file, "utf8");
      return {
        status: 200,
        body: { file: params.file, size: stat.size, mtime: stat.mtime.toISOString(), content: data }
      };
    } catch (error) {
      return { status: error.code === "ENOENT" ? 404 : 500, body: { error: error.message } };
    }
  }
};


/* ---------- global env ---------- */
async function readGlobalEnv() {
  try {
    return validateShellEnvironment(JSON.parse(await fs.readFile(GLOBAL_ENV, "utf8")));
  } catch (error) {
    if (error.code === "ENOENT") return Object.create(null);
    throw new Error(`global environment is invalid: ${error.message}`);
  }
}
async function writeGlobalEnv(value) {
  const clean = validateShellEnvironment(value);
  await fs.mkdir(path.dirname(GLOBAL_ENV), { recursive: true, mode: 0o700 });
  const temporary = `${GLOBAL_ENV}.tmp-${process.pid}-${Date.now()}`;
  let handle;
  try {
    handle = await fs.open(temporary, "wx", 0o600);
    await handle.writeFile(`${JSON.stringify(clean, null, 2)}\n`, "utf8");
    await handle.sync();
    await handle.close();
    handle = null;
    await fs.rename(temporary, GLOBAL_ENV);
  } finally {
    if (handle) await handle.close().catch(() => {});
    await fs.rm(temporary, { force: true }).catch(() => {});
  }
}

setGlobalEnvProvider(readGlobalEnv);
let globalEnvUpdateOperation = Promise.resolve();
let globalEnvRefreshRunning = false;
let globalEnvVersion = 0;
let pendingGlobalEnvRefresh = null;
let globalEnvRefreshStatus = { state: "idle", desiredVersion: 0, appliedVersion: 0, failures: [] };

function serializeGlobalEnvUpdate(operation) {
  const pending = globalEnvUpdateOperation.then(operation, operation);
  globalEnvUpdateOperation = pending.catch(() => {});
  return pending;
}

function startGlobalEnvRefresh() {
  if (globalEnvRefreshRunning) return;
  globalEnvRefreshRunning = true;
  void (async () => {
    while (pendingGlobalEnvRefresh) {
      const job = pendingGlobalEnvRefresh;
      pendingGlobalEnvRefresh = null;
      globalEnvRefreshStatus = {
        ...globalEnvRefreshStatus,
        state: "propagating",
        desiredVersion: Math.max(globalEnvRefreshStatus.desiredVersion, job.version)
      };
      try {
        const result = await refreshGlobalEnvironment(
          job.previous,
          job.next,
          () => job.version === globalEnvVersion
        );
        if (job.version !== globalEnvVersion) continue;
        globalEnvRefreshStatus = {
          state: result.failures.length ? "degraded" : "current",
          desiredVersion: job.version,
          appliedVersion: job.version,
          failures: result.failures
        };
        if (result.failures.length) {
          console.error(`[reaper] global environment propagation failed for projects: ${result.failures.join(", ")}`);
        }
      } catch {
        if (job.version !== globalEnvVersion) continue;
        globalEnvRefreshStatus = {
          ...globalEnvRefreshStatus,
          state: "degraded",
          appliedVersion: job.version,
          failures: ["GLOBAL_REFRESH_FAILED"]
        };
        console.error("[reaper] global environment propagation failed");
      }
    }
  })().finally(() => {
    globalEnvRefreshRunning = false;
    if (pendingGlobalEnvRefresh) startGlobalEnvRefresh();
  });
}

function enqueueGlobalEnvRefresh(previous, next, version) {
  pendingGlobalEnvRefresh = pendingGlobalEnvRefresh
    ? { previous: pendingGlobalEnvRefresh.previous, next, version }
    : { previous, next, version };
  globalEnvRefreshStatus = {
    ...globalEnvRefreshStatus,
    state: "pending",
    desiredVersion: version
  };
  startGlobalEnvRefresh();
}

const globalEnv = {
  get: async () => ({ status: 200, body: { env: await readGlobalEnv() } }),
  set: async (req, body) => {
    const clean = validateShellEnvironment(body?.env ?? Object.create(null));
    return serializeGlobalEnvUpdate(async () => {
      const previous = await readGlobalEnv();
      await writeGlobalEnv(clean);
      const version = ++globalEnvVersion;
      enqueueGlobalEnvRefresh(previous, clean, version);
      return { status: 200, body: { ok: true, count: Object.keys(clean).length } };
    });
  }
};

/* ---------- health ---------- */
const health = async () => ({ status: 200, body: { status: "ok" } });
const readiness = async () => {
  const reasons = [];
  if (localShellStatus.degraded) reasons.push("TERMINAL_RUNTIME_DEGRADED");
  if (globalEnvRefreshStatus.state === "degraded") reasons.push("GLOBAL_ENV_PROPAGATION_DEGRADED");
  return {
    status: 200,
    body: {
      status: "ready",
      degraded: reasons.length > 0,
      reasons,
      runtimeFailures: localShellStatus.failures?.length || 0,
      globalEnvFailures: globalEnvRefreshStatus.failures.length,
      propagation: globalEnvRefreshStatus
    }
  };
};

/* ---------- router ---------- */
/* bot protection + per-resource api tokens */
/* ---------- per-resource API tokens + bot protection ---------- */
const API_TOKENS_FILE = path.join(STATE_DIR, "api-tokens.json");
const PROJECT_API_TOKENS_DIR = path.join(STATE_DIR, "project-api-tokens");
const TOKEN_SCOPE_NAMES = new Set(["read", "write", "exec"]);

function validTokenRecord(record) {
  return Boolean(
    record && typeof record === "object" && !Array.isArray(record) &&
    typeof record.id === "string" && record.id.length > 0 && record.id.length <= 128 &&
    typeof record.name === "string" && record.name.length <= 64 &&
    typeof record.prefix === "string" && /^rpat_[a-f0-9]{7}$/.test(record.prefix) &&
    typeof record.hash === "string" && /^[a-f0-9]{64}$/.test(record.hash) &&
    Array.isArray(record.scopes) && record.scopes.every((scope) => TOKEN_SCOPE_NAMES.has(scope)) &&
    Number.isFinite(record.createdAt) && Number.isFinite(record.expiresAt) &&
    (record.revokedAt === null || Number.isFinite(record.revokedAt)) &&
    (record.lastUsedAt === null || Number.isFinite(record.lastUsedAt))
  );
}

function readTokenRecords(file) {
  try {
    const parsed = JSON.parse(fss.readFileSync(file, "utf8"));
    return Array.isArray(parsed) ? parsed.filter(validTokenRecord) : [];
  } catch {
    return [];
  }
}

function writeTokenRecords(file, records) {
  fss.mkdirSync(path.dirname(file), { recursive: true });
  const temporary = `${file}.tmp-${process.pid}-${crypto.randomBytes(6).toString("hex")}`;
  try {
    fss.writeFileSync(temporary, JSON.stringify(records, null, 2), { mode: 0o600 });
    fss.renameSync(temporary, file);
  } finally {
    try { fss.rmSync(temporary, { force: true }); } catch {}
  }
}

function projectTokenFile(name) {
  if (!PROJECT_NAME_RE.test(name || "")) throw new Error("invalid project token scope");
  return path.join(PROJECT_API_TOKENS_DIR, `${name}.json`);
}

state.globalApiTokens = readTokenRecords(API_TOKENS_FILE);
function saveGlobalTokens() { writeTokenRecords(API_TOKENS_FILE, state.globalApiTokens); }

function generateApiToken() { return "rpat_" + crypto.randomBytes(32).toString("hex"); }
function hashToken(t) { return crypto.createHash("sha256").update(t).digest("hex"); }

function tokenStoreFor(scope) {
  if (scope.kind === "global") return { list: state.globalApiTokens, save: saveGlobalTokens };
  if (scope.kind !== "project") throw new Error("invalid token scope");
  const file = projectTokenFile(scope.name);
  const list = readTokenRecords(file);
  return { list, save: () => writeTokenRecords(file, list) };
}

function deleteProjectTokenStore(name) {
  fss.rmSync(projectTokenFile(name), { force: true });
}

function createTokenV2(scope, name, scopes, ttlDays, ownerId) {
  const raw = generateApiToken();
  const rec = {
    id: crypto.randomUUID(),
    scopes, name,
    prefix: raw.slice(0, 12),
    hash: hashToken(raw),
    createdAt: Date.now(),
    expiresAt: Date.now() + ttlDays * 86400_000,
    revokedAt: null, lastUsedAt: null,
    ownerId
  };
  const store = tokenStoreFor(scope);
  store.list.push(rec);
  store.save();
  return { record: rec, raw };
}

function listTokensForScope(scope) {
  const store = tokenStoreFor(scope);
  return store.list.map((t) => ({
    id: t.id, name: t.name, scopes: t.scopes, prefix: t.prefix,
    createdAt: t.createdAt, expiresAt: t.expiresAt,
    revokedAt: t.revokedAt, lastUsedAt: t.lastUsedAt
  }));
}

function revokeTokenInScope(scope, id) {
  const store = tokenStoreFor(scope);
  const t = store.list.find((x) => x.id === id);
  if (!t) return null;
  t.revokedAt = Date.now();
  store.save();
  return t;
}

function rotateTokenInScope(scope, id, ttlDays) {
  const store = tokenStoreFor(scope);
  const t = store.list.find((x) => x.id === id);
  if (!t || t.revokedAt) return null;
  const raw = generateApiToken();
  t.hash = hashToken(raw);
  t.prefix = raw.slice(0, 12);
  t.expiresAt = Date.now() + ttlDays * 86400_000;
  t.lastUsedAt = null;
  store.save();
  return { record: t, raw };
}

function tokenHashMatches(record, expectedHash) {
  if (!validTokenRecord(record)) return false;
  return crypto.timingSafeEqual(Buffer.from(record.hash, "hex"), Buffer.from(expectedHash, "hex"));
}

function findToken(raw) {
  if (!raw || !raw.startsWith("rpat_")) return null;
  const expectedHash = hashToken(raw);
  const global = state.globalApiTokens.find((record) => tokenHashMatches(record, expectedHash));
  if (global) return { ...global, scope: { kind: "global", name: null } };
  try {
    for (const entry of fss.readdirSync(PROJECT_API_TOKENS_DIR, { withFileTypes: true })) {
      if (!entry.isFile() || !entry.name.endsWith(".json")) continue;
      const name = entry.name.slice(0, -5);
      if (!PROJECT_NAME_RE.test(name)) continue;
      const record = readTokenRecords(path.join(PROJECT_API_TOKENS_DIR, entry.name))
        .find((candidate) => tokenHashMatches(candidate, expectedHash));
      if (record) return { ...record, scope: { kind: "project", name } };
    }
  } catch {}
  return null;
}

function getApiTokenUser(req) {
  const auth = req.headers.authorization || "";
  let raw = null;
  if (auth.toLowerCase().startsWith("bearer ")) raw = auth.slice(7).trim();
  else if (req.headers["x-api-token"]) raw = String(req.headers["x-api-token"]).trim();
  if (!raw || !raw.startsWith("rpat_")) return null;
  const tok = findToken(raw);
  if (!tok) return null;
  if (tok.revokedAt) return null;
  if (tok.expiresAt && Date.now() > tok.expiresAt) return null;
  tok.lastUsedAt = Date.now();
  return {
    id: tok.id,
    username: (tok.name || "token") + "@" + (tok.scope?.kind || "global") + ":" + (tok.scope?.name || "global"),
    role: tok.scope?.kind === "global" ? "admin" : "api",
    scope: tok.scope,
    scopes: Array.isArray(tok.scopes) ? tok.scopes.filter((scope) => ["read", "write", "exec"].includes(scope)) : ["read"]
  };
}

function requiredApiTokenScope(match) {
  const route = match.pattern.startsWith(match.method + " ")
    ? match.pattern.slice(match.method.length + 1)
    : match.pattern;
  if (/\/(?:api-tokens|tokens)(?:\/|$)/.test(route)) return null;
  if (route.startsWith("/api/auth/")) return null;
  if (match.method === "GET" || match.method === "HEAD") return "read";
  if (route.includes("/sessions")) return "exec";
  return "write";
}

function tokenAllowsRequest(apiUser, match) {
  if (!apiUser?.scope) return { allowed: false, reason: "missing token resource scope" };
  const required = requiredApiTokenScope(match);
  if (!required) return { allowed: false, reason: "API tokens cannot access this route" };
  if (!apiUser.scopes.includes(required)) {
    return { allowed: false, reason: `token requires ${required} scope`, required };
  }
  const scope = apiUser.scope;
  if (scope.kind === "global") return { allowed: true, required };
  if (scope.kind === "project" && match.pattern.startsWith("/api/projects/:name")) {
    return { allowed: match.params.name === scope.name, reason: "token not authorized for this project", required };
  }
  return { allowed: false, reason: "token not authorized for this resource type", required };
}

/* ---------- global rate limit ---------- */
const ipBuckets = new Map();
const RATE_BUCKET_WINDOW = Number(process.env.GLOBAL_RATE_LIMIT_WINDOW_MS || 60_000);
const RATE_BUCKET_MAX = Number(process.env.GLOBAL_RATE_LIMIT_MAX || 240);
const FORWARD_AUTH_RATE_MAX = Number(process.env.FORWARD_AUTH_RATE_MAX || 10_000);
let ipBucketsLastPrunedAt = 0;
let forwardAuthWindowAt = Date.now();
let forwardAuthCount = 0;
if (!Number.isSafeInteger(RATE_BUCKET_WINDOW) || RATE_BUCKET_WINDOW < 1000) throw new Error("GLOBAL_RATE_LIMIT_WINDOW_MS must be an integer of at least 1000");
if (!Number.isSafeInteger(RATE_BUCKET_MAX) || RATE_BUCKET_MAX < 1) throw new Error("GLOBAL_RATE_LIMIT_MAX must be a positive integer");
if (!Number.isSafeInteger(FORWARD_AUTH_RATE_MAX) || FORWARD_AUTH_RATE_MAX < 1) throw new Error("FORWARD_AUTH_RATE_MAX must be a positive integer");

function isLoopbackRequest(req) {
  const address = String(req.socket?.remoteAddress || "");
  return address === "127.0.0.1" || address === "::1" || address === "::ffff:127.0.0.1";
}

function checkForwardAuthRateLimit(req) {
  if (!isLoopbackRequest(req)) return false;
  const now = Date.now();
  if (now - forwardAuthWindowAt >= RATE_BUCKET_WINDOW) {
    forwardAuthWindowAt = now;
    forwardAuthCount = 0;
  }
  if (forwardAuthCount >= FORWARD_AUTH_RATE_MAX) return false;
  forwardAuthCount += 1;
  return true;
}

function checkGlobalRateLimit(req) {
  const ip = clientIp(req);
  const now = Date.now();
  ipBucketsLastPrunedAt = pruneRateLimitStore(ipBuckets, now, RATE_BUCKET_WINDOW, ipBucketsLastPrunedAt);
  const current = ipBuckets.get(ip);
  const bucket = !current || now - current.startedAt >= RATE_BUCKET_WINDOW
    ? { startedAt: now, count: 0, lastSeenAt: now }
    : current;
  if (bucket.count >= RATE_BUCKET_MAX) {
    bucket.lastSeenAt = now;
    touchRateLimitEntry(ipBuckets, ip, bucket);
    return false;
  }
  bucket.count += 1;
  bucket.lastSeenAt = now;
  touchRateLimitEntry(ipBuckets, ip, bucket);
  return true;
}

/* ---------- bot detection ---------- */
const BOT_PATTERNS = [
  /bot/i, /crawler/i, /spider/i, /scrape/i, /curl\//i, /wget/i,
  /python-requests/i, /go-http-client/i, /node-fetch/i, /axios\//i,
  /headless/i, /phantom/i, /puppeteer/i, /playwright/i,
  /scrapy/i, /httrack/i, /libwww/i
];
const ALLOWED_BOT_UAS = (process.env.ALLOWED_BOT_UAS || "").split(",").map((s) => s.trim()).filter(Boolean);
function looksLikeBot(ua) {
  if (!ua) return true;
  return BOT_PATTERNS.some((re) => re.test(ua));
}
function isAllowedBot(ua) {
  return ALLOWED_BOT_UAS.some((allowed) => ua.includes(allowed));
}

/* ---------- token handlers ---------- */
function tokenCredentialManagementDenied(req) {
  return req.apiUser
    ? { status: 403, body: { error: "API tokens cannot manage credentials" } }
    : null;
}

function makeTokenHandlers(scopeKind, getNameParam) {
  return {
    list: async (req, body, params) => {
      const denied = tokenCredentialManagementDenied(req);
      if (denied) return denied;
      const name = getNameParam(req, params || {});
      const scope = { kind: scopeKind, name };
      if (scopeKind === "project") assertProjectAccess(name);
      return { status: 200, body: { tokens: listTokensForScope(scope) } };
    },
    create: async (req, body, params) => {
      const denied = tokenCredentialManagementDenied(req);
      if (denied) return denied;
      const name = getNameParam(req, params || {});
      const scope = { kind: scopeKind, name };
      if (scopeKind === "project") assertProjectAccess(name);
      const scopes = [...new Set(Array.isArray(body?.scopes) ? body.scopes.filter((candidate) => TOKEN_SCOPE_NAMES.has(candidate)) : ["read"])];
      const ttlDays = Math.max(1, Math.min(3650, Number(body?.ttlDays) || 365));
      const tname = String(body?.name || "token").trim().slice(0, 64);
      if (!tname || /[\u0000-\u001f\u007f]/.test(tname)) return { status: 400, body: { error: "invalid token name" } };
      const { record, raw } = createTokenV2(scope, tname, scopes, ttlDays, req.user.username);
      await audit("token.create", req, { scope, name: tname, scopes });
      return { status: 201, body: { id: record.id, name: record.name, scopes: record.scopes, prefix: record.prefix, expiresAt: record.expiresAt, token: raw } };
    },
    revoke: async (req, body, params) => {
      const denied = tokenCredentialManagementDenied(req);
      if (denied) return denied;
      const name = getNameParam(req, params);
      const scope = { kind: scopeKind, name };
      if (scopeKind === "project") assertProjectAccess(name);
      const t = revokeTokenInScope(scope, params.id);
      if (!t) return { status: 404, body: { error: "token not found" } };
      await audit("token.revoke", req, { scope, id: params.id });
      return { status: 200, body: { ok: true } };
    },
    rotate: async (req, body, params) => {
      const denied = tokenCredentialManagementDenied(req);
      if (denied) return denied;
      const name = getNameParam(req, params);
      const scope = { kind: scopeKind, name };
      if (scopeKind === "project") assertProjectAccess(name);
      const ttlDays = Math.max(1, Math.min(3650, Number(body?.ttlDays) || 365));
      const out = rotateTokenInScope(scope, params.id, ttlDays);
      if (!out) return { status: 404, body: { error: "token not found" } };
      await audit("token.rotate", req, { scope, id: params.id });
      return { status: 200, body: { id: out.record.id, prefix: out.record.prefix, expiresAt: out.record.expiresAt, token: out.raw } };
    }
  };
}

const globalTokens = makeTokenHandlers("global", () => null);
const projectTokens = makeTokenHandlers("project", (req, p) => p.name);


const routes = [
  ["POST",   "/api/auth/login", false, auth.login],
  ["POST",   "/api/auth/logout", true, auth.logout],
  ["GET",    "/api/auth/csrf", true, auth.csrf],
  ["GET",    "/api/auth/me", true, auth.me],
  ["GET",    "/api/health", false, health],
  ["GET",    "/api/ready", false, readiness],
  ["GET",    "/api/global-env", true, globalEnv.get],
  ["PUT",    "/api/global-env", true, globalEnv.set],
  ["GET",    "/api/projects", true, projects.list],
  ["POST",   "/api/projects", true, projects.create],
  ["GET",    "/api/projects/:name", true, projects.get],
  ["DELETE", "/api/projects/:name", true, projects.remove],
  ["GET",    "/api/projects/:name/files", true, files.tree],
  ["GET",    "/api/projects/:name/file", true, files.read],
  ["PUT",    "/api/projects/:name/file", true, files.write],
  ["DELETE", "/api/projects/:name/file", true, files.delete],
  ["POST",   "/api/projects/:name/dir", true, files.mkdir],
  ["POST",   "/api/projects/:name/upload", true, files.upload, true],
  ["GET",    "/api/projects/:name/download", true, files.download, true],
  ["HEAD",   "/api/projects/:name/download", true, files.download, true],
  ["GET",    "/api/projects/:name/env", true, env.get],
  ["PUT",    "/api/projects/:name/env", true, env.set],
  ["GET",    "/api/projects/:name/bashrc", true, env.getBashrc],
  ["PUT",    "/api/projects/:name/bashrc", true, env.setBashrc],
  ["GET",    "/api/projects/:name/sessions", true, sessions.list],
  ["POST",   "/api/projects/:name/sessions", true, sessions.create],
  ["PATCH",  "/api/projects/:name/sessions/:s", true, sessions.rename],
  ["DELETE", "/api/projects/:name/sessions/:s", true, sessions.destroy],
  ["GET",    "/api/projects/:name/ports", true, ports.get],
  ["PUT",    "/api/projects/:name/ports", true, ports.put],
  ["GET",    "/api/audit", true, async () => ({ status: 200, body: { audit: state.audit || [] } })],
  ["GET",    "/api/api-tokens", true, globalTokens.list],
  ["POST",   "/api/api-tokens", true, globalTokens.create],
  ["DELETE", "/api/api-tokens/:id", true, globalTokens.revoke],
  ["POST",   "/api/api-tokens/:id/rotate", true, globalTokens.rotate],
  ["GET",    "/api/projects/:name/tokens", true, projectTokens.list],
  ["POST",   "/api/projects/:name/tokens", true, projectTokens.create],
  ["DELETE", "/api/projects/:name/tokens/:id", true, projectTokens.revoke],
  ["POST",   "/api/projects/:name/tokens/:id/rotate", true, projectTokens.rotate],
];

function decodeRouteComponent(value, parameter) {
  let decoded;
  try { decoded = decodeURIComponent(value); }
  catch { throw httpError(400, `invalid percent encoding in ${parameter}`); }
  if (parameter === "name" && !isDirectChildName(decoded)) {
    throw httpError(400, "invalid resource name");
  }
  return decoded;
}

function findRoute(method, pathname) {
  for (const [m, pattern, auth, handler, raw] of routes) {
    if (m !== method) continue;
    const pParts = pattern.split("/");
    const uParts = pathname.split("/");
    if (pParts.length !== uParts.length) continue;
    const params = {};
    let matched = true;
    for (let i = 0; i < pParts.length; i++) {
      if (pParts[i].startsWith(":")) {
        const parameter = pParts[i].slice(1);
        params[parameter] = decodeRouteComponent(uParts[i], parameter);
      } else if (pParts[i] !== uParts[i]) {
        matched = false;
        break;
      }
    }
    if (matched) return { handler, params, auth, method: m, pattern, raw };
  }
  return null;
}
const matchRoute = findRoute;

function parseQs(url) {
  const i = url.indexOf("?");
  if (i < 0) return { qs: "", query: {} };
  const qs = url.slice(i + 1);
  const query = {};
  for (const part of qs.split("&")) {
    const separator = part.indexOf("=");
    const rawKey = separator < 0 ? part : part.slice(0, separator);
    const rawValue = separator < 0 ? "" : part.slice(separator + 1);
    if (rawKey) query[decodeRouteComponent(rawKey, "query")] = decodeRouteComponent(rawValue, "query");
  }
  return { qs, query };
}

async function handleRequest(req, res) {
  const url = req.url || "/";
  const i = url.indexOf("?");
  const pathname = i < 0 ? url : url.slice(0, i);
  setCors(res, req.headers.origin);

  let query;
  try { ({ query } = parseQs(url)); }
  catch (error) { return sendJson(res, error.statusCode || 400, { error: error.message }); }

  if (req.method === "OPTIONS") { res.statusCode = 204; return res.end(); }
  const forwardAuthRequest = pathname === "/api/auth/me" && checkForwardAuthRateLimit(req);
  if (!forwardAuthRequest && !checkGlobalRateLimit(req)) return sendJson(res, 429, { error: "rate limit exceeded" });

  const apiUser = getApiTokenUser(req);
  const ua = req.headers["user-agent"] || "";
  if (!apiUser && req.method === "GET" && !getUser(req) && pathname !== "/api/health" && pathname !== "/api/ready") {
    if (looksLikeBot(ua) && !isAllowedBot(ua)) {
      return sendJson(res, 403, { error: "forbidden", reason: "bot detection" });
    }
  }

  let match;
  try { match = matchRoute(req.method, pathname); }
  catch (error) { return sendJson(res, error.statusCode || 400, { error: error.message }); }
  if (!match) return sendJson(res, 404, { error: "not found", path: pathname });

  if (match.auth) {
    if (apiUser) {
      req.user = apiUser;
      req.apiToken = true;
      const authorization = tokenAllowsRequest(apiUser, match);
      if (!authorization.allowed) {
        await audit("token.forbidden", req, {
          path: pathname,
          scope: apiUser.scope,
          requiredScope: authorization.required,
          reason: authorization.reason
        });
        return sendJson(res, 403, { error: authorization.reason });
      }
    } else {
      const user = getUser(req);
      if (!user) return sendJson(res, 401, { error: "unauthorized" });
      req.user = user;
      if (!checkCsrf(req)) return sendJson(res, 403, { error: "csrf token missing or invalid" });
    }
  }

  try {
    if (match.raw) return await match.handler(req, res, match.params, query);
    let body = {};
    if (req.method !== "GET" && req.method !== "HEAD") body = await readBody(req);
    const result = await match.handler(req, body, match.params, query);
    if (!result) return;
    if (result.cookies?.access) setAuthCookie(res, result.cookies.access);
    if (result.cookies?.csrf) setCsrfCookieImpl(res, result.cookies.csrf);
    if (result.cookies?.clear) clearAuthCookie(res);
    return sendJson(res, result.status, result.body);
  } catch (error) {
    const status = error.statusCode || (error.name === "ZodError" ? 400 : 500);
    if (status >= 500) console.error(`[${req.method} ${pathname}]`, error);
    return sendJson(res, status, { error: error.message, ...(error.detail || {}) });
  }
}

/* ---------- boot ---------- */
const server = http.createServer(handleRequest);
const terminalWss = new WebSocketServer({ noServer: true, perMessageDeflate: false, maxPayload: 64 * 1024 });
server.on("upgrade", (req, socket, head) => {
  let pathname;
  try { pathname = new URL(req.url || "/", "http://reaper.local").pathname; }
  catch { socket.destroy(); return; }
  const user = getUser(req);
  if (pathname !== "/terminal/ws" || !isAllowedSocketOrigin(req) || !user) {
    socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
    socket.destroy();
    return;
  }
  req.reaperAuth = user;
  terminalWss.handleUpgrade(req, socket, head, (ws) => terminalWss.emit("connection", ws, req));
});
attachTerminalWebSocket(terminalWss, {
  verifyCsrf: (req, token) => getCsrfTokens(req).some((candidate) => constantTimeEqual(candidate, token))
});
localShellStatus = await initLocalShells();
server.listen(PORT, "0.0.0.0", () => {
  console.log(`[reaper] listening on http://0.0.0.0:${PORT}`);
  console.log(`[reaper] projects: ${VPS_PROJECTS}`);
  console.log(`[reaper] global:   ${GLOBAL_ENV}`);
});
let shutdownRequested = false;
async function shutdownServer() {
  if (shutdownRequested) return;
  shutdownRequested = true;
  await shutdownLocalShells();
  const closed = new Promise((resolve) => server.close(resolve));
  server.closeAllConnections?.();
  await closed;
}
function requestShutdown() {
  void shutdownServer().then(
    () => process.exit(0),
    (error) => {
      console.error("[reaper] shutdown failed:", error);
      process.exit(1);
    }
  );
}
process.on("SIGINT", requestShutdown);
process.on("SIGTERM", requestShutdown);
process.on("uncaughtException", (e) => console.error("[reaper] uncaught", e));



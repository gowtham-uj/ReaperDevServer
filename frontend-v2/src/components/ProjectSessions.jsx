import { createEffect, createSignal, For, Show } from "solid-js";
import { api } from "../api.js";

const SESSION_NAME = /^[a-z0-9-]{1,32}$/;
const SUBDOMAIN = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

function sessionName(session) {
  return String(session?.name || session?.sessionName || session?.sessionId || "").split("/").pop();
}

function normalizePort(port = {}) {
  return {
    containerPort: port.containerPort == null ? "" : String(port.containerPort),
    subdomain: String(port.subdomain || "")
  };
}

function isIpHostname(hostname) {
  const host = String(hostname || "").replace(/^\[|\]$/g, "");
  if (host.includes(":")) return true;
  const octets = host.split(".");
  return octets.length === 4 && octets.every((octet) => /^\d{1,3}$/.test(octet) && Number(octet) <= 255);
}

export function ProjectSessions(props) {
  const [sessions, setSessions] = createSignal([]);
  const [sessionsState, setSessionsState] = createSignal("loading");
  const [sessionError, setSessionError] = createSignal("");
  const [busySession, setBusySession] = createSignal("");
  const [editingSession, setEditingSession] = createSignal("");
  const [editingTitle, setEditingTitle] = createSignal("");
  const [creatingSession, setCreatingSession] = createSignal(false);
  const [newSessionName, setNewSessionName] = createSignal("");
  const [ports, setPorts] = createSignal([]);
  const [savedPorts, setSavedPorts] = createSignal("[]");
  const [requireReaperAuth, setRequireReaperAuth] = createSignal(true);
  const [portsState, setPortsState] = createSignal("loading");
  const [portsError, setPortsError] = createSignal("");
  let sessionLoadSequence = 0;
  const projectPath = () => `/api/projects/${encodeURIComponent(props.name)}`;

  async function loadSessions() {
    const loadSequence = ++sessionLoadSequence;
    setSessionsState("loading");
    setSessionError("");
    try {
      const body = await api(`${projectPath()}/sessions`);
      if (loadSequence !== sessionLoadSequence) return [];
      const next = Array.isArray(body?.sessions) ? body.sessions : [];
      setSessions(next);
      setSessionsState("ready");
      return next;
    } catch (error) {
      if (loadSequence === sessionLoadSequence) {
        setSessionsState("error");
        setSessionError(error?.message || "Sessions could not be loaded.");
      }
      return [];
    }
  }

  async function loadPorts() {
    setPortsState("loading");
    setPortsError("");
    try {
      const body = await api(`${projectPath()}/ports`);
      const next = (Array.isArray(body?.ports) ? body.ports : []).map(normalizePort);
      const nextRequireReaperAuth = body?.requireReaperAuth !== false;
      setPorts(next);
      setRequireReaperAuth(nextRequireReaperAuth);
      setSavedPorts(JSON.stringify({ ports: next, requireReaperAuth: nextRequireReaperAuth }));
      setPortsState("ready");
    } catch (error) {
      setPortsState("error");
      setPortsError(error?.message || "Published ports could not be loaded.");
    }
  }

  createEffect(() => {
    props.name;
    void loadSessions();
    void loadPorts();
  });


  async function createSession(event) {
    if (busySession()) return;
    event?.preventDefault();
    const name = newSessionName().trim().toLowerCase().replace(/[^a-z0-9-]+/g, "-").replace(/^-+|-+$/g, "");
    if (!SESSION_NAME.test(name)) {
      setSessionError("Session names must be 1–32 lowercase letters, numbers, or hyphens.");
      return;
    }
    if (sessions().some((session) => sessionName(session) === name)) {
      setSessionError(`Session “${name}” already exists.`);
      document.getElementById(`session-row-${name}`)?.focus();
      return;
    }
    setBusySession(name);
    setSessionError("");
    try {
      await api(`${projectPath()}/sessions`, { method: "POST", body: JSON.stringify({ name }) });
      setCreatingSession(false);
      setNewSessionName("");
      await loadSessions();
      props.onSessionsChange?.();
      queueMicrotask(() => document.getElementById(`session-row-${name}`)?.focus());
    } catch (error) {
      setSessionError(error?.message || "Session could not be created.");
    } finally {
      setBusySession("");
    }
  }

  function beginRename(session) {
    const name = sessionName(session);
    setEditingSession(name);
    setEditingTitle(String(session.title || name));
    setSessionError("");
    queueMicrotask(() => document.getElementById(`session-title-${name}`)?.select());
  }

  async function renameSession(event, session) {
    event.preventDefault();
    if (busySession()) return;
    const name = sessionName(session);
    const title = editingTitle().trim();
    if (!title || title.length > 48) {
      setSessionError("Session titles must be 1–48 characters.");
      return;
    }
    setBusySession(name);
    try {
      await api(`${projectPath()}/sessions/${encodeURIComponent(name)}`, {
        method: "PATCH",
        body: JSON.stringify({ title })
      });
      setEditingSession("");
      await loadSessions();
      props.onSessionsChange?.();
      queueMicrotask(() => document.getElementById(`session-row-${name}`)?.focus());
    } catch (error) {
      setSessionError(error?.message || "Session title could not be saved.");
    } finally {
      setBusySession("");
    }
  }

  async function destroy(session) {
    if (busySession()) return;
    const name = sessionName(session);
    const title = session.title || name;
    if (!window.confirm(`Delete persistent session “${title}” (${name})? This stops every process running in it.`)) return;
    const index = sessions().indexOf(session);
    setBusySession(name);
    setSessionError("");
    try {
      await api(`${projectPath()}/sessions/${encodeURIComponent(name)}`, { method: "DELETE" });
      const remaining = await loadSessions();
      props.onSessionsChange?.();
      const next = remaining[Math.min(index, Math.max(0, remaining.length - 1))];
      queueMicrotask(() => {
        if (next) document.getElementById(`session-row-${sessionName(next)}`)?.focus();
        else document.getElementById("create-persistent-session")?.focus();
      });
    } catch (error) {
      setSessionError(error?.message || "Session could not be deleted.");
      queueMicrotask(() => document.getElementById(`session-row-${name}`)?.focus());
    } finally {
      setBusySession("");
    }
  }

  function updatePort(index, field, value) {
    setPorts((current) => current.map((row, rowIndex) => rowIndex === index ? { ...row, [field]: value } : row));
    setPortsError("");
  }

  function portValidation() {
    const seenPorts = new Set();
    const seenSubdomains = new Set();
    for (let index = 0; index < ports().length; index += 1) {
      const row = ports()[index];
      const port = Number(row.containerPort);
      const subdomain = row.subdomain.trim().toLowerCase();
      if (!Number.isInteger(port) || port < 1 || port > 65535) return `Row ${index + 1}: container port must be an integer from 1 to 65535.`;
      if (usesIpPublishing() && (port < 1024 || port === 2019 || port === 4000)) {
        return `Row ${index + 1}: IP publishing requires an available host port from 1024 to 65535.`;
      }
      if (!SUBDOMAIN.test(subdomain)) return `Row ${index + 1}: route name must use lowercase letters, numbers, or internal hyphens.`;
      if (seenPorts.has(port)) return `Row ${index + 1}: container port ${port} is already published.`;
      if (seenSubdomains.has(subdomain)) return `Row ${index + 1}: route name “${subdomain}” is already used.`;
      seenPorts.add(port);
      seenSubdomains.add(subdomain);
    }
    return "";
  }

  const dirty = () => JSON.stringify({ ports: ports(), requireReaperAuth: requireReaperAuth() }) !== savedPorts();
  const validationError = () => portValidation();

  async function savePorts(event) {
    event.preventDefault();
    const validation = validationError();
    if (validation) {
      setPortsError(validation);
      return;
    }
    setPortsState("saving");
    setPortsError("");
    const payload = ports().map((row) => ({
      containerPort: Number(row.containerPort),
      subdomain: row.subdomain.trim().toLowerCase()
    }));
    try {
      const body = await api(`${projectPath()}/ports`, {
        method: "PUT",
        body: JSON.stringify({ ports: payload, requireReaperAuth: requireReaperAuth() })
      });
      const next = (Array.isArray(body?.ports) ? body.ports : payload).map(normalizePort);
      const nextRequireReaperAuth = body?.requireReaperAuth !== false;
      setPorts(next);
      setRequireReaperAuth(nextRequireReaperAuth);
      setSavedPorts(JSON.stringify({ ports: next, requireReaperAuth: nextRequireReaperAuth }));
      setPortsState("saved");
      setTimeout(() => setPortsState((state) => state === "saved" ? "ready" : state), 1800);
    } catch (error) {
      setPortsState("error");
      setPortsError(error?.message || "Published ports could not be saved.");
    }
  }

  function usesIpPublishing() {
    return typeof window !== "undefined" && isIpHostname(window.location.hostname);
  }

  function publishedUrl(port = {}) {
    const subdomain = port.subdomain || "route";
    if (typeof window === "undefined") return `https://${subdomain}.example.com`;
    if (!usesIpPublishing()) return `https://${subdomain}.${window.location.hostname}`;
    const hostname = window.location.hostname.includes(":") && !window.location.hostname.startsWith("[")
      ? `[${window.location.hostname}]`
      : window.location.hostname;
    return `${window.location.protocol}//${hostname}:${port.containerPort || "port"}/`;
  }

  return (
    <div class="project-sessions-page">
      <section aria-labelledby="sessions-title">
        <div class="row project-sessions-head">
          <div>
            <div class="page__eyebrow">Sessions</div>
            <h2 id="sessions-title" class="project-sessions-title">Persistent terminal sessions</h2>
            <p class="muted project-sessions-lede">Named tmux sessions stay warm when browsers disconnect and backend deployments complete.</p>
          </div>
          <div class="spacer" />
          <Show when={!creatingSession()} fallback={
            <form class="session-create-form" onSubmit={(event) => void createSession(event)}>
              <div class="field">
                <label class="field__label" for="persistent-session-name">Session name</label>
                <input
                  id="persistent-session-name"
                  class="input"
                  value={newSessionName()}
                  maxlength="32"
                  pattern="[a-z0-9-]{1,32}"
                  placeholder="server"
                  autocomplete="off"
                  required
                  onInput={(event) => setNewSessionName(event.currentTarget.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Escape") {
                      event.preventDefault();
                      setCreatingSession(false);
                      setNewSessionName("");
                      queueMicrotask(() => document.getElementById("create-persistent-session")?.focus());
                    }
                  }}
                />
              </div>
              <button class="btn btn--primary" type="submit" disabled={Boolean(busySession())}>Create session</button>
              <button
                class="btn btn--ghost"
                type="button"
                onClick={() => {
                  setCreatingSession(false);
                  setNewSessionName("");
                  queueMicrotask(() => document.getElementById("create-persistent-session")?.focus());
                }}
              >Cancel</button>
            </form>
          }>
            <button
              id="create-persistent-session"
              class="btn btn--primary"
              type="button"
              disabled={Boolean(busySession())}
              onClick={() => {
                setCreatingSession(true);
                setSessionError("");
                queueMicrotask(() => document.getElementById("persistent-session-name")?.focus());
              }}
            >+ New session</button>
          </Show>
        </div>
        <Show when={sessionError()}><p class="field__error" role="alert">{sessionError()}</p></Show>
        <Show when={sessionsState() !== "loading"} fallback={<div class="muted" role="status">Loading sessions…</div>}>
          <Show when={sessionsState() !== "error"} fallback={<div class="empty-state" role="alert"><h3>Sessions could not be loaded</h3><p>{sessionError()}</p><button class="btn btn--outline" type="button" onClick={() => void loadSessions()}>Retry</button></div>}>
            <Show when={sessions().length} fallback={<div class="card empty"><div class="empty__title">No sessions</div><p>Create a named persistent session to start a shell.</p></div>}>
              <div class="project-sessions-table-wrap">
                <table class="token-table project-sessions-table">
                  <thead><tr><th>Session</th><th>State</th><th>Clients</th><th>Last activity</th><th><span class="sr-only">Actions</span></th></tr></thead>
                  <tbody>
                    <For each={sessions()}>{(session) => {
                      const name = sessionName(session);
                      return (
                        <tr>
                          <td>
                            <Show when={editingSession() !== name} fallback={
                              <form class="session-title-edit" onSubmit={(event) => void renameSession(event, session)}>
                                <label class="sr-only" for={`session-title-${name}`}>Title for session {name}</label>
                                <input id={`session-title-${name}`} class="input" maxlength="48" value={editingTitle()} onInput={(event) => setEditingTitle(event.currentTarget.value)} onKeyDown={(event) => { if (event.key === "Escape") { event.preventDefault(); setEditingSession(""); queueMicrotask(() => document.getElementById(`session-row-${name}`)?.focus()); } }} />
                                <button class="btn btn--primary btn--sm" type="submit" disabled={Boolean(busySession())}>Save</button>
                                <button class="btn btn--ghost btn--sm" type="button" onClick={() => { setEditingSession(""); queueMicrotask(() => document.getElementById(`session-row-${name}`)?.focus()); }}>Cancel</button>
                              </form>
                            }>
                              <button id={`session-row-${name}`} class="session-title-button" type="button" disabled={Boolean(busySession())} onClick={() => beginRename(session)} aria-label={`Rename session ${session.title || name}`}>
                                <span>{session.title || name}</span><span class="mono muted">{name}</span>
                              </button>
                            </Show>
                          </td>
                          <td><span class={`status-dot ${session.state === "running" ? "status-dot--ok" : "status-dot--warn"}`} aria-hidden="true"></span> {session.state || "ready"}</td>
                          <td class="muted">{session.attachedClients || 0}</td>
                          <td class="muted">{session.lastInteractionAt ? new Date(session.lastInteractionAt).toLocaleString() : "—"}</td>
                          <td><button class="btn btn--ghost btn--sm session-delete" type="button" disabled={Boolean(busySession())} onClick={() => void destroy(session)} aria-label={`Delete persistent session ${session.title || name}`}>{busySession() === name ? "Deleting…" : "Delete"}</button></td>
                        </tr>
                      );
                    }}</For>
                  </tbody>
                </table>
              </div>
            </Show>
          </Show>
        </Show>
      </section>

      <section class="published-ports" aria-labelledby="published-ports-title">
        <div>
          <div class="page__eyebrow">Networking</div>
          <h2 id="published-ports-title" class="project-sessions-title">Published ports</h2>
          <p id="published-ports-help" class="muted project-sessions-lede">
            {usesIpPublishing()
              ? "Publish a container port directly on this server’s IP address. Ports below 1024 and Reaper’s own ports are reserved."
              : "Publish a container port at a project subdomain. Wildcard DNS and TLS must be configured by the server administrator."}
          </p>
        </div>
        <Show when={portsState() !== "loading"} fallback={<div class="muted" role="status">Loading published ports…</div>}>
          <Show when={portsState() !== "error"} fallback={<div class="empty-state" role="alert"><h3>Published ports could not be loaded</h3><p>{portsError()}</p><button class="btn btn--outline" type="button" onClick={() => void loadPorts()}>Retry</button></div>}>
          <form class="published-ports-form" aria-describedby="published-ports-help" onSubmit={(event) => void savePorts(event)}>
            <div class="published-port-list">
              <For each={ports()}>{(row, index) => (
                <fieldset class="published-port-row">
                  <legend class="sr-only">Published port {index() + 1}</legend>
                  <div class="field published-port-field">
                    <label class="field__label" for={`container-port-${index()}`}>Container port</label>
                    <input id={`container-port-${index()}`} class="input" type="number" inputmode="numeric" min="1" max="65535" step="1" required value={row.containerPort} aria-invalid={validationError() && (!Number.isInteger(Number(row.containerPort)) || Number(row.containerPort) < 1 || Number(row.containerPort) > 65535) ? "true" : undefined} onInput={(event) => updatePort(index(), "containerPort", event.currentTarget.value)} />
                  </div>
                  <div class="field published-port-field published-port-field--subdomain">
                    <label class="field__label" for={`port-subdomain-${index()}`}>Route name</label>
                    <input id={`port-subdomain-${index()}`} class="input" type="text" required maxlength="63" autocomplete="off" placeholder="app" value={row.subdomain} aria-invalid={row.subdomain && !SUBDOMAIN.test(row.subdomain) ? "true" : undefined} onInput={(event) => updatePort(index(), "subdomain", event.currentTarget.value.toLowerCase())} />
                    <a class="published-port-url" href={SUBDOMAIN.test(row.subdomain) ? publishedUrl(row) : undefined} target="_blank" rel="noreferrer">{publishedUrl(row)}</a>
                  </div>
                  <button class="btn btn--ghost published-port-remove" type="button" onClick={() => setPorts((current) => current.filter((_, rowIndex) => rowIndex !== index()))} aria-label={`Remove published port row ${index() + 1}`}>Remove</button>
                </fieldset>
              )}</For>
            </div>
            <div class="published-auth-setting">
              <label class="published-auth-control" for="require-reaper-auth">
                <input
                  id="require-reaper-auth"
                  type="checkbox"
                  checked={requireReaperAuth()}
                  aria-describedby="require-reaper-auth-help"
                  onChange={(event) => {
                    setRequireReaperAuth(event.currentTarget.checked);
                    setPortsError("");
                  }}
                />
                <span>
                  <span class="published-auth-label">Require Reaper sign-in</span>
                  <span id="require-reaper-auth-help" class="field__help">
                    Disable only when the published application enforces its own authentication. Routes will be reachable without a Reaper session.
                  </span>
                </span>
              </label>
              <Show when={!requireReaperAuth()}>
                <aside class="published-auth-warning" aria-label="Authentication warning">
                  <strong>Reaper sign-in is disabled.</strong> Anyone who can reach a published route can access it unless the application requires its own authentication.
                </aside>
              </Show>
            </div>
            <button class="btn btn--outline published-port-add" type="button" onClick={() => setPorts((current) => [...current, normalizePort()])}>+ Add port</button>
            <Show when={portsError() || validationError()}><p class="field__error" role="alert">{portsError() || validationError()}</p></Show>
            <div class="published-ports-actions">
              <span class="muted" role="status" aria-live="polite">{portsState() === "saving" ? "Saving published ports…" : portsState() === "saved" ? "Published ports saved." : dirty() ? "Unsaved changes" : "All changes saved"}</span>
              <button class="btn btn--primary" type="submit" disabled={!dirty() || Boolean(validationError()) || portsState() === "saving"}>{portsState() === "saving" ? "Saving…" : "Save ports"}</button>
            </div>
          </form>
          </Show>
        </Show>
      </section>
    </div>
  );
}

# Reaper V2

Reaper is a self-hosted browser workspace for durable project files, isolated project runtimes, and persistent terminal sessions.

## Runtime architecture

- **Caddy** terminates HTTPS, serves `frontend-v2/dist`, proxies `/api/*` and `/terminal/ws`, and loads generated routes for published project ports.
- **Backend** is a Node.js API on loopback port `4000`. It owns authentication, project files and settings, terminal control, project-pod lifecycle, and Caddy route generation.
- **Project pod** is one durable `reaper-pod-*` Docker container per visible workspace directory. It mounts only that project at `/work`; the Docker socket and other projects are not mounted.
- **Terminal session** is one named `tmux` session inside the project pod. A project may have multiple persistent sessions.
- `workspace` stores project directories. `backend-state` stores users, login sessions, API tokens, project control metadata, terminal manifests, and archived logs; live terminal logs remain inside the durable project pod.

The retired Theia, code-server, shared OpenCode server, PostgreSQL, monitoring worker, and legacy browser IDE are not part of this stack.

## Terminal persistence contract

Closing a browser tab, navigating away, losing the network, or restarting the Reaper backend only detaches the viewer. The project pod, `tmux` session, foreground process, working directory, and terminal log remain live. Reopening the terminal replays captured output before live output starts, then attaches the viewer to the same session. Multiple viewers can attach concurrently.

Every project starts with a `main` session. Additional sessions have validated names and optional display titles. Rename changes only the title. The explicit session **Delete** action is destructive: Reaper stops the exact `tmux` session and every process carrying that session identity, archives its log, removes its manifest record, and notifies attached viewers. Deleting a project first closes viewers and stops/removes the project pod, then removes project state and files; per-project operations are serialized so queued work cannot recreate a deleted runtime.

The pod's PID 1 supervisor is a child subreaper. If an interactive shell exits, the supervisor reaps descendants and starts a clean shell in the same persistent session. Reaper never replays arbitrary commands: replay could duplicate destructive or billable effects.

The persistence boundary is the running project pod:

- Reaper deployments rebuild/recreate the backend and Caddy but deliberately leave `reaper-pod-*` containers untouched.
- Project files, environment settings, Bash startup configuration, session manifests, and captured logs survive process and service restarts.
- Restarting or deleting a project pod kills its in-memory processes. Reaper reconstructs declared terminal sessions, not arbitrary PIDs, sockets, unsaved process memory, or TUI internals.
- Local non-Linux development falls back to a subprocess shell for tests and basic development; production fails closed unless the Docker/tmux pod backend is available.

## Terminal transport and rendering

The browser and backend use the binary RTP/1 WebSocket protocol at `/terminal/ws`. Frames carry explicit type, stream ID, sequence, flags, and payload length. Session open ordering is `OPENED`, history chunks, `READY`, then live output. Cumulative acknowledgements, bounded unacknowledged output, socket-buffer limits, and a backpressure timeout prevent a stalled viewer from growing backend memory without bound. Heartbeats and authentication expiry close stale sockets.

Input remains raw bytes end to end; UTF-8 decoder state is preserved across split frames. Terminal output is never routed back into shell input. Resize frames target the exact `tmux` session. A disconnect only detaches the viewer and never sends `exit`, `Ctrl-C`, or `Ctrl-D`.

The UI uses xterm.js with WebGL and canvas fallback, Unicode 11 cell widths, bundled JetBrains Mono, search, copy/paste controls, 50,000 lines of browser scrollback, and a truecolor terminal palette. The canonical terminal grid is shared by viewers; smaller desktop and mobile viewports pan or scroll without changing the running process unless a deliberate resize is sent.

## Isolation and trust boundary

Project pods receive configured memory, CPU, and PID limits. Each pod receives its own labelled, collision-resistant bridge network with inter-container communication disabled; Docker assigns the pod address from a deterministic non-overlapping subnet. Pod creation and every published route verify container ownership, network isolation, and address placement, then fail closed on drift. The backend only exposes explicitly published container ports; generated Caddy routes authenticate first and strip Reaper cookies before proxying to the project application.

Backend file APIs reject traversal and symlink escapes. Sensitive project control data and project API-token stores live under `backend-state`, not under user-controlled workspace paths. Project tokens are scoped to one project and cannot manage interactive credentials.

The backend runs as root with access to the Docker daemon socket. That is a host-level trust boundary, not a hostile multi-tenant sandbox. Do not give untrusted users Reaper accounts or direct access to the host.

## Deployment

Requirements: Linux, Docker Engine with the Compose plugin, wildcard DNS for published subdomains when `APEX_DOMAIN` is used, and a built frontend.

Copy the environment template and set the required secrets:

```sh
cd /app
cp .env.example .env
```

```env
APP_ADMIN_USERNAME=
APP_ADMIN_PASSWORD=replace-with-a-strong-password
JWT_ACCESS_SECRET=replace-with-a-long-random-secret
APEX_DOMAIN=example.com
COOKIE_DOMAIN=
```

`APP_ADMIN_PASSWORD` must be at least 12 characters and `JWT_ACCESS_SECRET` must be a long random value. Keep `COOKIE_DOMAIN` empty for host-only session cookies. Published wildcard subdomains currently require a shared domain cookie and therefore must host only trusted applications; do not treat them as hostile multi-tenant origins.

Build the frontend, then run the guarded deployment:

```sh
npm ci --prefix frontend-v2
npm run build --prefix frontend-v2
bash ops/deploy.sh
```

`ops/deploy.sh` validates Compose and the pod-network security invariant before changing services, creates a bounded control-plane backup, stages the frontend for an atomic cutover, builds `reaper-pod:latest`, and runs Compose. It never stops, removes, or recreates project pods; per-project private networks are created and verified on demand.

Verify:

```sh
docker compose ps
curl -fsS http://127.0.0.1:4000/api/health
curl -kfsS https://127.0.0.1/health
```

Do not delete `workspace`, `backend-state`, `terminal-home`, `opencode`, `caddy-data`, or `caddy-config` during an upgrade.

## Development and focused verification

```sh
cd backend
npm install
node --test src/services/terminal-protocol.test.js \
  src/services/pod-runtime.test.js \
  src/services/local-shell.test.js \
  src/server.test.js

cd ../frontend-v2
npm install
npm run build
```

The C supervisor and pod entrypoint are compiled and exercised by building `pod-image`. End-to-end terminal verification should cover browser/network reconnect, browser close/reopen, backend restart, concurrent viewers, UTF-8 input, named-session create/delete, and confirmation that the terminal process remains the same where persistence is promised.

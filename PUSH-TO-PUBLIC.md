# Pre-push credential rotation checklist

This repository does **not** contain any host, IP, username, or password
specific to any individual deployment. The following values, however, must
be supplied at deploy time and must not be committed.

Rotate or supply these at deploy time:

- `APP_ADMIN_PASSWORD` (set in `app/.env`) — a strong password of at
  least 12 characters. The server rejects placeholder values
  (`change-me`, `change_me`, `placeholder`, `example-password`).
- `JWT_ACCESS_SECRET` (set in `app/.env`) — a random secret of at least
  32 characters. The server rejects placeholder values
  (`change-me`, `change_me`, `placeholder`, `example-secret`).
- `APEX_DOMAIN` (set in `app/.env`) — the public hostname that resolves
  to this host. It is the only public hostname the deployment exposes.
  Caddy will fail fast at boot if it is not set.
- `COOKIE_DOMAIN` (optional, `app/.env`) — leave empty for host-only
  session cookies; set only if the deployment uses a registered
  subdomain and you want cookies scoped to it.
- **TLS issuer account** — the Let's Encrypt account used by Caddy is
  tied to a contact email. Use a neutral address owned by the
  operator.
- **VPS host SSH key** — your personal deploy key. Never commit it.
  Rotate if it was ever used in a public context.
- **Project API tokens** — issue only after the public push. Any tokens
  created during local testing must be revoked.

`app/.env`, `.reaper-local/`, `backend/.reaper-local/`, and
`backend-state/` are excluded by `.gitignore`. Confirm before pushing:

```sh
git ls-files | grep -E '(\.env$|users\.json|audit\.log|sessions\.json|reaper-ssh-password)' || echo "clean"
```

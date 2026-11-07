# Put a stdio MCP server behind byward

byward can run a stdio MCP server itself and serve it over Streamable HTTP, behind the same
sign-in as an HTTP upstream. An upstream process only ever acts for one signed-in user
([ADR-0003](../adr/0003-upstream-kinds.md)):

- **MCP 2025-11-25** (VS Code, Cursor, MCP Inspector, and dual-era clients that fall back to it):
  each MCP session gets its own upstream process.
- **MCP 2026-07-28**, if your server speaks it: all of a user's requests, from all their MCP
  clients, share one upstream process for that user. byward asks the server `server/discover` on
  its first 2026-07-28 request. A server that does not answer it is treated as 2025-11-25 only,
  and clients fall back to that.

Linux and macOS only, one replica.

## 1. Give the command

After the flags, put `--` and the command:

```sh
byward --public-url https://mcp.example.com/mcp [other settings] -- /usr/local/bin/my-server --read-only
```

Or set it in the environment as a JSON array:

```sh
BYWARD_UPSTREAM_COMMAND='["/usr/local/bin/my-server","--read-only"]'
```

The command is never run through a shell. Give it either way, not both, and never together with
`BYWARD_UPSTREAM_URL`. A command that is not found stops startup with exit code 2.

## 2. Environment of the upstream process

The process does **not** inherit the proxy's environment. It gets only:

| Variables | From |
|---|---|
| `PATH`, `HOME`, `USER`, `LOGNAME`, `TMPDIR`, `TZ`, `LANG`, `LC_ALL`, `LC_CTYPE`, `LC_MESSAGES` | the proxy's environment, if set |
| your variables | `BYWARD_UPSTREAM_ENV_FILE` (below) |
| `BYWARD_SUBJECT`, `BYWARD_ISSUER` | the proxy: the signed-in user's key (`tid:oid` for Entra) and the proxy's issuer URL |

So no `BYWARD_*` setting, sealing key or cloud credential of the proxy reaches the upstream. The
user's identity is never in the arguments, and no token ever reaches the process: it acts as your
service identity, and the proxy decides who may reach it.

Give the upstream its own settings and secrets in a file of `KEY=VALUE` lines (blank lines and
lines starting with `#` are skipped):

```sh
# /run/secrets/upstream-env, mode 0600
API_KEY=sk-...
LOG_LEVEL=info
```

```sh
BYWARD_UPSTREAM_ENV_FILE=/run/secrets/upstream-env
```

Like every secret file, it must not be readable by group or others, or the proxy refuses to start
(ADR-0018 §11; `--dev` relaxes this). The values are never logged; the startup log shows `<redacted>`. Names starting with `BYWARD_` are
refused, so an upstream can trust that `BYWARD_SUBJECT` comes from the proxy. Use the command for
non-secret options: its arguments are shown in the startup log.

## 3. Isolate state between sessions

Every session runs a fresh process, but processes share the host. Servers that keep state in a fixed
place (a `memory` server's file, a SQLite database, a browser profile, a git checkout) let one
session see another's data, including another user's. Give each process its own directory with the
`{session_dir}` placeholder, in an argument or an env-file value. The proxy creates the directory
before the process starts and removes it when the process exits:

```sh
byward ... -- /usr/local/bin/memory-server --file "{session_dir}/memory.json"
```

Browser servers need `--isolated` or `--user-data-dir={session_dir}`.

## 4. Limits

| Setting | Default | What happens at the limit |
|---|---|---|
| `BYWARD_STDIO_MAX_PROCESSES` | 64 | The least recently used idle process (any user's) is stopped so a new session can start |
| `BYWARD_STDIO_MAX_PROCESSES_PER_USER` | 8 | That user's own least recently used idle process is stopped; other users' processes are never touched |

A process is idle when no request or stream of its own is open: a GET stream of its MCP session,
or any 2026-07-28 request of its user. If nothing is idle, the new session or request gets `503`
with `Retry-After: 10`, and the proxy logs `stdio process cap full: every process is in use`. Each stop under a limit writes a `process_evicted`
audit record; the evicted session's client gets `404` and starts a new session. A process that is
stopping still counts until it has exited.

A process also stops after 30 minutes without traffic, counted from when its last request or
stream ended; a process in use never idles out, even with no traffic.

**Long-lived streams hold slots.** An open GET stream or a long 2026-07-28 request keeps its
process in use for as long as the client keeps it open, and a process in use is never evicted. So
one user can hold up to `BYWARD_STDIO_MAX_PROCESSES_PER_USER` slots indefinitely, and that many
users together can fill `BYWARD_STDIO_MAX_PROCESSES`, after which everyone else gets `503`. With the
defaults (8 and 64) that is 8 users. If your users keep streams open, raise the upstream limit or
lower the per-user one; watch `byward_stdio_processes` and the log line above. Size the limits by memory: a Node or Python
server is 120–240 MB per process, about 40 % of it the `npx`/`uvx` launcher. Install the server in
the image and run it directly where you can.

Some clients start a new session for almost every tool call (claude.ai chat) and few ever end one,
so per-user processes pile up until the idle timeout or the per-user limit reclaims them.

## 5. What clients see

| Situation | Client gets |
|---|---|
| `initialize` without a session | A new process and an `Mcp-Session-Id` (sealed to the user when authentication is on) |
| Any other request without a session | `400` with a JSON-RPC error; dual-era clients fall back to `initialize` |
| A 2026-07-28 request, and the server answers `server/discover` | Served by the user's upstream process; the client's request ids and progress tokens are mapped to the proxy's own on the way in and back on the way out, so the user's clients never see each other's answers |
| A 2026-07-28 request, and the server does not answer `server/discover` | The same `400`; byward asks again after 10 minutes |
| A message (either era: a request, a notification or a response) with a top-level key repeated or the method in another case (`METHOD`); a 2026-07-28 request with a key at the top level, in `params` or in `params._meta` that is repeated, or that differs only in case from a key byward reads (`ID`), or an id or progress token that is not a string or an integer | `400`: the proxy does not guess which value is meant |
| A 2026-07-28 request whose client closes its stream | The upstream gets `notifications/cancelled` for it |
| A 2026-07-28 request whose `MCP-Protocol-Version`, `Mcp-Method` or `Mcp-Name` header is missing or does not match its body | `400` with `HeaderMismatch` (`-32020`): byward checks these for the server, which never sees headers |
| `subscriptions/listen` | Its stream carries the acknowledgement and the notifications the client asked for, under the client's own request id; it ends when the client closes it (the upstream gets `notifications/cancelled`), when the upstream answers it, or when the upstream tears it down |
| A `subscriptions/listen` stream or a session's GET stream with nothing to send for 15 s | A comment line (`:`), which clients ignore, keeps it open through proxies that cut quiet connections: nginx `proxy_read_timeout` and an AWS ALB's idle timeout default to 60 s, Cloudflare's proxy read timeout to 125 s. If the comment cannot be written, the client is gone: a listen is cancelled on the upstream |
| `GET` or `DELETE` without a session | `405`: only a 2025-11-25 MCP session has them |
| A session that ended, or another user's | `404`; the client starts a new session |
| The upstream process crashes, exits or closes its stdout | Open requests get a JSON-RPC error (`-32603`, `upstream process exited`), later ones `404`. The process is not restarted; one that closed stdout but keeps running is stopped after 5 s |
| The upstream asks the client something (sampling, elicitation, roots) while no stream is open | The proxy answers the upstream with a JSON-RPC error (`-32603`, `client not connected`) |
| `DELETE` with the session | `204`; the process is stopped |
| An `MCP-Protocol-Version` header other than the version the upstream's `InitializeResult` negotiated for this MCP session | `400` with a JSON-RPC error; no header at all passes (2025-11-25 transports, Protocol Version Header) |

Messages the upstream sends during a request go out on that request's stream. Messages outside any
request go to the session's GET stream; a second GET replaces the first. Until a GET stream is open,
up to 256 notifications wait. Streams cannot be resumed (`Last-Event-ID` is ignored).

The upstream's stderr is logged, one record per line, tagged with the upstream, the process ID and
the user key. Lines are cut at 8 KiB and rate-limited per process. Make sure your server never
writes credentials to stderr.

## 6. Container image

The byward image has no Node or Python. Build your own image from the runtime and copy the binary in
([ADR-0019](../adr/0019-packaging.md)):

```dockerfile
FROM node:26-slim
RUN npm install -g @modelcontextprotocol/server-everything@2.2.1
COPY --from=ghcr.io/byward-dev/byward:<version> /usr/local/bin/byward /usr/local/bin/byward
COPY --from=ghcr.io/byward-dev/byward:<version> --chown=node:node /var/lib/byward /var/lib/byward
USER node
ENTRYPOINT ["/usr/local/bin/byward", "--", "mcp-server-everything"]
```

```dockerfile
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim
ENV UV_TOOL_DIR=/opt/uv-tools UV_TOOL_BIN_DIR=/usr/local/bin
RUN uv tool install mcp-server-time==2026.11.4
COPY --from=ghcr.io/byward-dev/byward:<version> /usr/local/bin/byward /usr/local/bin/byward
COPY --from=ghcr.io/byward-dev/byward:<version> --chown=65532:65532 /var/lib/byward /var/lib/byward
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/byward", "--", "mcp-server-time"]
```

Pin both images by digest in production. The repository's tested example is
[`docs/examples/stdio-node/Dockerfile`](../examples/stdio-node/Dockerfile).

## Without authentication

With `--insecure-no-auth` ([ADR-0027](../adr/0027-insecure-no-auth.md)) **anyone who can reach the
proxy starts upstream processes**, bounded only by `BYWARD_STDIO_MAX_PROCESSES`, and every process
runs as the same anonymous user (`BYWARD_SUBJECT=anonymous`, `BYWARD_ISSUER=urn:byward:no-auth`).
Session IDs are not sealed. The proxy logs this as a warning at every start. Use it only to try
a server out on your own machine.

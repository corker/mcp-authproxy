# ADR-0024: Testing strategy — a Docker-free check, a client simulator, conformance three ways

- Status: Accepted
- Date: 2026-11-06
- Issue: #22 (F21)

## Context

Coding agents run `mise run check` in sandboxes (ADR-0002); the proxy supports two spec revisions,
several clients and IdPs, and Entra first-class.

Standards and tools (2026-11-06):
- **MCP conformance:** `0.2.0-alpha.12` (2026-10-01) carries the 2026-07-28 requirements; stable
  `0.1.16` is from 2026-03-30. Its 2026-07-28 requirements have "deliberately no authorization-server
  section", the `server` command cannot send a bearer token, and `authorization` runs an interactive
  code grant.
- **OpenID conformance suite:** OP plans only; we issue no ID tokens.
- **Go:** `testing/synctest` is GA since Go 1.25 (`Sleep` since 1.27).
- **Accessibility:** no official axe integration for Go; `@axe-core/playwright` 4.14.0 is official.
- **GitHub Actions:** fork PRs "typically you can't grant write access" (so no `id-token: write`);
  repositories created after 2026-07-15 get an immutable ID-based OIDC `sub`, and this one was created
  2026-11-05.
- **Inspector CLI 2.14.0:** OAuth with a shared store and `--stored-auth-only` for CI.

Competitors (source):
- babs: tests 2.3× its code, a Keycloak e2e, threats linked to tests.
- ToolHive: MCP conformance run through its proxy.
- Pomerium: synctest, and the MCP auth checks re-implemented in Go.
- dex: a storage conformance suite.

None has a client-quirk simulator, a scheduled real-IdP lane, chaos tests or mutation testing. Neither
mockoidc nor mock-oauth2-server has an Entra profile.

Runtime (2026-11-06, Go 1.26.5, Apple M4 Pro, loopback): a proxied request added about 84 µs over a
direct one; opening a sealed token took 0.3 µs, signing the ES256 assertion 17 µs.

## Decision

1. **Lanes:**

   | Lane | Docker | Trigger | Contents |
   |---|---|---|---|
   | `test` (in `check`) | no | every commit | unit; fuzz seed corpus; fake IdP profiles; fake HTTP and stdio upstreams; client simulator; synctest chaos; property tests; Go assertion contract; store suite on memory and SQLite; migration fixtures; limit and cap tests; `doctor` misconfiguration tests |
   | `test:integration` | yes | PR, required | Keycloak container; MCP conformance (item 4); real open-source clients (item 3); TypeScript and FastMCP assertion verifiers; Playwright with axe on the consent and error pages; image smoke test; restore drill |
   | scheduled | yes | nightly or weekly | real Entra via GitHub OIDC; `go test -fuzz` 2 min per target; load test; reproducible-build rebuild |
   | manual | — | release | Claude Code, Claude.ai, VS Code, Cursor, ChatGPT; OBO |

2. **Client simulator:** one scripted profile per closed client, from verified behaviour: discovery
   order, CIMD or DCR, redirect form, `resource` sent or not, 403 handling, Claude Code's
   `headersHelper` on 401. Profiles are versioned data, one per compatibility-matrix cell, and the
   manual release run updates them.
3. **Real open-source clients** in integration: the TypeScript and Python SDK clients (including
   Python's client credentials), the go-sdk client and Inspector CLI, logged in through the fake IdP.
4. **MCP conformance, pinned (`0.2.0-alpha.12` until a stable release has the 2026-07-28
   requirements):**
   - **(a)** `server` for both eras against the reference server behind our proxy and a test-only
     token injector, proving byte-transparency; _Amended by ADR-0027:_ run against the binary in
     `--insecure-no-auth` mode; no token injector is needed for this lane.
   - **(b)** `authorization` against the broker, driven by fake-IdP auto-login and a scripted consent
     POST;
   - **(c)** resource-server behaviour (401 shapes, protected-resource metadata, `resource`) in our own
     Go tests.
5. **Fake IdP profiles** in `internal/testutil`:
   - **entra:** v2 `aud` = client ID, `tid`/`oid`, groups overage, AADSTS errors, `correlation_id`
     echo, app-only tokens with and without `roles`, `idtyp`;
   - **generic OIDC**, and Google `hd`;
   - **Keycloak** runs as the real container.
6. **Tests that guard decisions:**
   - redaction of logs;
   - no telemetry connection when `OTEL_*` is unset;
   - golden files for the audit schema, metric names and assertion claims;
   - the generated configuration reference is current;
   - the Entra quickstart needs five or fewer keys;
   - token-format dispatch never falls back to another validator.
7. **Invariants are linked to tests:** a table in `docs/quality-lenses.md` maps each invariant to its
   tests, and a test fails when an invariant has none. The table is added with the first tests.
8. **Time:** `testing/synctest` and an injected clock; a wall-clock sleep in a test fails review.
9. **Fuzz targets:** sealed tokens, redirect URIs, CIMD documents, era detection, token-format dispatch.
10. **Performance:** Go benchmarks on the hot path (token open, deny-list check, `Rewrite`, assertion
    signing), compared with the previous run; a scheduled load test against a budget of 2 ms p99 added
    latency per MCP request.
11. **Upgrade and recovery:** a store fixture per released schema version, migrated to the current one;
    a restore drill (snapshot, write, restore) checks that refresh tokens are refused and the restore is
    audited (ADR-0007).
12. **CI hardening:**
    - least-privilege `permissions` by default;
    - `id-token: write` only in the scheduled real-Entra workflow on `main` (`schedule`,
      `workflow_dispatch`), never `pull_request_target`;
    - the federated credential uses the `sub` printed by a first run and is bound to `main` or an
      environment.
13. **Fixture privacy:** fixtures use generated GUIDs only; recorded traffic goes through the redactor;
    a test rejects identifiers listed in a CI variable (never in the repository) and email-shaped
    strings.
14. **Mutation testing** (gremlins, 0.x) by hand on `internal/token` and `internal/broker`; not in CI.
15. **Budgets:**
    - `check` ≤ 90 s on a warm laptop and ≤ 3 min in CI;
    - `test:integration` ≤ 10 min;
    - line coverage ≥ 80 % in `internal/token`, `internal/broker` and `internal/idp`, with no global
      gate.
16. **CI runs on Linux** (ADR-0019).

## Consequences

- Headless runs of `conformance authorization` and Inspector CLI are unproven until the broker exists
  (slice 1). Fallbacks: Go re-implementations of the checks (Pomerium), and the SDK clients.
- OBO stays a manual check: a headless Entra user login needs a test user with an MFA exclusion.
- The scheduled real-Entra lane depends on the maintainer's tenant and never blocks a PR.
- `docs/quality-lenses.md` is corrected: conformance usage and platforms in the compatibility matrix,
  and the budgets decided by ADR-0006, ADR-0010 and ADR-0018.

## Considered options

- Running the conformance `--suite auth`: rejected; it tests MCP clients, not servers.
- axe through chromedp in Go: rejected; unofficial, and Node is already in the integration lane.
- Mocking Entra with mockoidc or mock-oauth2-server: rejected; no Entra profile.
- A stored Entra user refresh token for an automated OBO test: rejected; it is a stored secret.

## Amendment 2026-11-06 (real open-source clients; Keycloak deferred)

- **§3 is built** (slice 2.3, #58). The clients sign in with static registration and call a tool on
  a real MCP upstream (go-sdk), on the `test:integration:go` lane:

  | Client | Version | How it runs |
  |---|---|---|
  | go-sdk | v1.8.0 | in process |
  | TypeScript SDK | 1.32.2 | pinned container |
  | Python SDK `mcp` | 2.3.0 | pinned container |
  | Inspector CLI | 2.12.0 | pinned container; its own OAuth, which runs on the v2 SDK `@modelcontextprotocol/client` 2.3.1 |

  - byward runs in-process with its Entra adapter reaching the fake IdP through its HTTP client, so
    no IdP endpoint is configurable.
  - A scripted browser answers the consent page.
  - DCR follows #57. CIMD stays with the Go server tests and the client simulator (#59).
  - Python's client credentials wait for app-only access (v0.2).
- **Keycloak (§1, §5) moves to the slice that wires the generic OIDC adapter.**
  - The adapter exists (`internal/idp/oidc`, #73), but no setting selects it yet; the server wires
    only Entra.
  - Until then, a Keycloak container could test that adapter in isolation, but not byward end to end.
- **§4b is built** (`TestIntegrationConformanceAuthorization`). The pinned conformance CLI's
  `authorization` suite runs against the broker:
  - AS metadata, and the authorization code grant with PKCE and `resource`, signed in through the
    consent page and the fake IdP by the scripted browser;
  - DPoP, which skips because byward advertises no DPoP (ADR-0011 §8).

  Every check must pass. A WARNING (a SHOULD) fails unless it is listed with a reason.
  - It runs on the Go integration lane, not the §4a shell lane. Signing in needs the in-process
    fake IdP, which the binary cannot reach.
  - The CLI is the §4a pin: `scripts/conformance/run.sh --install-only` builds it.

## Amendment 2026-11-07 (#164): the load test

§10's load test is `internal/server/load_test.go` (build tag `load`, `mise run test:load`), run
weekly by `.github/workflows/load.yml`. It sends the same requests to the upstream directly and
through the wired proxy, open loop at a fixed rate, in alternating rounds, and gates on the median
over seven rounds of the added p99; a companion test proves that 5 ms more fails the gate
([how-to](../how-to/load-test.md)).

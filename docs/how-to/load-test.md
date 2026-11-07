# Run and read the load test

[ADR-0024 §10](../adr/0024-testing-strategy.md) budgets the latency byward adds to an MCP request:
**2 ms at the 99th percentile**. The load test measures it. It runs weekly on a GitHub-hosted runner
(`.github/workflows/load.yml`), on demand (`workflow_dispatch`), and on a pull request that changes
the harness or the workflow. It never blocks other pull requests.

## What it measures

The harness (`internal/server/load_test.go`, behind the `load` build tag) runs everything in one
process, over loopback TCP:

- a minimal upstream that answers `initialize` (with a session ID), a tool call with a 2 KB result,
  and a streamed tool call of 10 events;
- the proxy, wired exactly as in production (`server.New` from the settings, with an HTTP upstream,
  the identity assertion and MCP session binding), with 50 users holding sealed proxy tokens. The
  IdP is never called: the tokens are sealed beforehand, and any IdP request fails the test.

The same requests, in a fixed mix (20 % `initialize`, 60 % tool calls, 20 % streamed), go to the
upstream **directly** and **through the proxy**:

- **Open loop at a fixed rate** (400 requests a second by default). Each request has a due time,
  and its latency is measured from then until the last byte of the response, so a slow response
  never delays the clock of the requests after it (no coordinated omission). The rate is well below
  saturation: the test measures latency, not throughput.
- **Rounds alternate** direct and proxied, 15 s each, the order flipping every pair so drift on the
  machine falls on both. The first pair warms up and is discarded; 7 pairs are measured.
- **Added latency** per round is the proxied p50 and p99 minus the direct ones. It includes the
  proxy's own hop to the upstream.
- **The gate** is the median of the 7 added p99s: at most 2 ms passes.

A second test, `TestLoadGateCatchesAnAdded5ms`, adds 5 ms to every proxied request and checks that
the gate then fails, so every run proves the gate can fail.

Every request must succeed; one failure fails the run.

## Run it locally

```sh
mise run test:load
```

About four minutes. Shorter, for a quick look:

```sh
go test -count=1 -v -tags=load -run ^TestLoad ./internal/server/ -args -load.rounds=3 -load.duration=5s
```

Flags: `-load.rate` (requests a second), `-load.rounds`, `-load.duration` (one round), `-load.out`
(the results file, default `build/load/results.json`).

A busy machine makes the tail of both sides worse, and the proxied side, with an extra hop, worse
still: run it on an otherwise idle machine, or trust the CI runner's result.

## Read the result

- The job summary has a table: per round, direct and proxied p50 and p99, and the added p50 and
  p99; above it the medians and the budget.
- The `load-results` artifact (kept 90 days) is `results.json`: the same numbers in nanoseconds,
  plus each side's p99.9 and request count, the commit, the Go version and the runner's CPU count.
- A scheduled failure opens one issue labelled `load-test`, or comments on the open one.

If the added p99 is over budget, compare the rounds: one bad round among good ones is the runner;
every round over is byward. Then look for what the proxy does per request that it did not before.

//go:build load

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/byward-dev/byward/internal/config"
	"github.com/byward-dev/byward/internal/mcpsession"
	"github.com/byward-dev/byward/internal/token"
)

// The scheduled load test (ADR-0024 §10): the latency the proxy adds to an MCP request, against a
// budget of 2 ms p99. The same requests go to the upstream directly and through the wired proxy, in
// this process over loopback, in alternating rounds; the gate is the median over the rounds of the
// added p99. mise run test:load runs it; .github/workflows/load.yml runs it weekly.
var (
	loadRate     = flag.Int("load.rate", 400, "requests per second, sent open loop at fixed times")
	loadRounds   = flag.Int("load.rounds", 7, "measured round pairs (direct and through the proxy)")
	loadDuration = flag.Duration("load.duration", 15*time.Second, "length of one round")
	loadOut      = flag.String("load.out", "../../build/load/results.json", "where the results are written")
)

// loadBudget is ADR-0024 §10's added p99 per MCP request.
const loadBudget = 2 * time.Millisecond

const (
	loadUsers  = 50 // distinct users, each with a sealed proxy token and an MCP session
	loadEvents = 10 // events in a streamed response
)

func TestLoadAddedLatencyStaysInBudget(t *testing.T) {
	r := runLoad(t, *loadRate, *loadRounds, *loadDuration, 0)
	r.write(t, *loadOut)
	t.Logf("added p50 %s, added p99 %s (median of %d rounds; budget %s)", r.AddedP50, r.AddedP99, len(r.Rounds), loadBudget)
	if r.AddedP99 > loadBudget {
		t.Fatalf("the proxy adds %s p99, over the %s budget (ADR-0024 §10)", r.AddedP99, loadBudget)
	}
}

// The gate can fail: 5 ms more in every proxied request is caught. Shorter rounds suffice, since
// the signal is well above the budget.
func TestLoadGateCatchesAnAdded5ms(t *testing.T) {
	r := runLoad(t, *loadRate, 3, 3*time.Second, 5*time.Millisecond)
	if r.AddedP99 <= loadBudget {
		t.Fatalf("with 5 ms added the gate passed: added p99 %s, budget %s", r.AddedP99, loadBudget)
	}
}

// loadResult is results.json.
type loadResult struct {
	Commit   string        `json:"commit,omitempty"`
	Go       string        `json:"go"`
	CPUs     int           `json:"cpus"`
	Rate     int           `json:"rate_per_second"`
	Round    time.Duration `json:"round_ns"`
	Rounds   []loadRound   `json:"rounds"`
	AddedP50 time.Duration `json:"added_p50_ns"`
	AddedP99 time.Duration `json:"added_p99_ns"`
	Budget   time.Duration `json:"budget_p99_ns"`
}

type loadRound struct {
	Direct, Proxy loadStats
	AddedP50      time.Duration `json:"added_p50_ns"`
	AddedP99      time.Duration `json:"added_p99_ns"`
}

type loadStats struct {
	Requests int           `json:"requests"`
	P50      time.Duration `json:"p50_ns"`
	P99      time.Duration `json:"p99_ns"`
	P999     time.Duration `json:"p999_ns"`
}

// runLoad measures rounds round pairs (after a discarded warm-up pair), each side for d at rate,
// with extra added to every proxied request (0, except to prove the gate).
func runLoad(t *testing.T, rate, rounds int, d time.Duration, extra time.Duration) loadResult {
	t.Helper()
	up := httptest.NewServer(loadUpstream())
	t.Cleanup(up.Close)
	proxy, tokens := loadProxy(t, up.URL, extra)
	direct := &loadSide{name: "direct", base: up.URL, client: loadClient()}
	via := &loadSide{name: "proxy", base: proxy.URL, host: "mcp.example.com", client: loadClient(), tokens: tokens}
	for _, s := range []*loadSide{direct, via} {
		s.openSessions(t)
	}
	res := loadResult{Commit: os.Getenv("GITHUB_SHA"), Go: runtime.Version(), CPUs: runtime.NumCPU(), Rate: rate, Round: d, Budget: loadBudget}
	for i := range rounds + 1 {
		// Alternate which side goes first, so drift on the machine falls on both.
		first, second := direct, via
		if i%2 == 1 {
			first, second = via, direct
		}
		a, b := first.round(t, rate, d), second.round(t, rate, d)
		if i == 0 {
			continue // warm-up: connections, caches, the JIT-free but cold runtime
		}
		ds, ps := a, b
		if first == via {
			ds, ps = b, a
		}
		res.Rounds = append(res.Rounds, loadRound{Direct: ds, Proxy: ps, AddedP50: ps.P50 - ds.P50, AddedP99: ps.P99 - ds.P99})
	}
	res.AddedP50 = median(res.Rounds, func(r loadRound) time.Duration { return r.AddedP50 })
	res.AddedP99 = median(res.Rounds, func(r loadRound) time.Duration { return r.AddedP99 })
	return res
}

// loadUpstream answers the fixed mix: initialize (with a session ID), a tool call with a 2 KB
// result, and a streamed tool call of loadEvents events. It records nothing, so it stays flat
// under load.
func loadUpstream() http.Handler {
	result := strings.Repeat("x", 2048)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
			Params struct {
				Name string `json:"name"`
			} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		switch {
		case req.Method == "initialize":
			w.Header().Set(mcpsession.Header, "upstream-session")
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"load","version":"0"}}}`, req.ID)
		case req.Params.Name == "stream":
			w.Header().Set("Content-Type", "text/event-stream")
			f, _ := w.(http.Flusher)
			for i := range loadEvents {
				_, _ = fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progress\":%d}}\n\n", i)
				if f != nil {
					f.Flush()
				}
			}
			_, _ = fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"id\":%d,\"result\":{}}\n\n", req.ID)
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":{"content":[{"type":"text","text":%q}]}}`, req.ID, result)
		}
	})
}

// loadProxy is the proxy wired as in production in front of upstream, with loadUsers sealed proxy
// tokens. The IdP is never called: tokens are pre-sealed, and any IdP request fails the test.
func loadProxy(t *testing.T, upstream string, extra time.Duration) (*httptest.Server, []string) {
	t.Helper()
	orig := idpHTTPClient
	idpHTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		t.Errorf("the load test reached the IdP: %s", r.URL.Host)
		return nil, errors.New("no IdP in the load test")
	})}
	t.Cleanup(func() { idpHTTPClient = orig })
	env := append(authEnv(t), "BYWARD_PUBLIC_URL=https://mcp.example.com/mcp", "BYWARD_UPSTREAM_URL="+upstream+"/mcp")
	cfg, _, err := config.Load(nil, env, os.ReadFile)
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(cfg, discard)
	if err != nil {
		t.Fatal(err)
	}
	// The background work, as Run starts it: above all the deny-list resync, without which every
	// token is refused once the list is 30 s stale. It stops before the store closes.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.background(ctx) }()
	t.Cleanup(func() { cancel(); <-done; _ = s.closeStore() })
	h := s.Public.Handler
	if extra > 0 {
		inner := h
		h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(extra); inner.ServeHTTP(w, r) })
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	ring, err := token.LoadKeyRing(cfg.KeysDir)
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := token.NewSealer(ring, cfg.PublicURL.String())
	if err != nil {
		t.Fatal(err)
	}
	tokens := make([]string, loadUsers)
	now := time.Now()
	for i := range tokens {
		tokens[i], err = sealer.Seal(token.Access, token.Claims{GrantID: fmt.Sprint("g", i), Subject: fmt.Sprintf("%s:%08d-0000-0000-0000-000000000000", fixtureTenant, i),
			ClientID: "test-app", Roles: []string{"byward.user"}, IssuedAt: now, ExpiresAt: now.Add(time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
	}
	return srv, tokens
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func loadClient() *http.Client {
	return &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConns: 1000, MaxIdleConnsPerHost: 1000, IdleConnTimeout: time.Minute}}
}

// loadSide is one way to the upstream: direct, or through the proxy with the users' tokens.
type loadSide struct {
	name, base, host string
	client           *http.Client
	tokens           []string // through the proxy only
	sessions         []string // per user, as initialize returned it on this side
}

func (s *loadSide) openSessions(t *testing.T) {
	t.Helper()
	s.sessions = make([]string, loadUsers)
	for u := range loadUsers {
		res, err := s.send(context.Background(), u, 0, "")
		if err != nil {
			t.Fatalf("%s: initialize for user %d: %v", s.name, u, err)
		}
		s.sessions[u] = res
	}
}

// send makes request k for user u and reads the whole response; it returns the session ID the
// response set (initialize only).
func (s *loadSide) send(ctx context.Context, u, k int, session string) (string, error) {
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"load","version":"0"}}}`, k)
	switch k % 5 {
	case 1, 2, 3:
		body = fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":"echo","arguments":{}}}`, k)
	case 4:
		body = fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":"stream","arguments":{}}}`, k)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.base+"/mcp", bytes.NewBufferString(body))
	if err != nil {
		return "", err
	}
	if s.host != "" {
		req.Host = s.host
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if s.tokens != nil {
		req.Header.Set("Authorization", "Bearer "+s.tokens[u])
	}
	if session != "" {
		req.Header.Set(mcpsession.Header, session)
	}
	res, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = res.Body.Close() }()
	if _, err := io.Copy(io.Discard, res.Body); err != nil {
		return "", err
	}
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", res.StatusCode)
	}
	return res.Header.Get(mcpsession.Header), nil
}

// round sends rate requests a second for d, open loop: request k is due at start + k/rate, and its
// latency counts from then, so a slow response delays no later request's clock (no coordinated
// omission). Every request must succeed.
func (s *loadSide) round(t *testing.T, rate int, d time.Duration) loadStats {
	t.Helper()
	runtime.GC()
	n := int(d.Seconds() * float64(rate))
	interval := time.Second / time.Duration(rate)
	lat := make([]time.Duration, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := time.Now()
	for k := range n {
		due := start.Add(time.Duration(k) * interval)
		time.Sleep(time.Until(due))
		u := k % loadUsers
		session := ""
		if k%5 != 0 {
			session = s.sessions[u]
		}
		wg.Go(func() {
			_, errs[k] = s.send(context.Background(), u, k, session)
			lat[k] = time.Since(due)
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatalf("%s: %d requests failed: %v", s.name, countErrs(errs), firstErr(errs))
	}
	slices.Sort(lat)
	return loadStats{Requests: n, P50: quantile(lat, 0.50), P99: quantile(lat, 0.99), P999: quantile(lat, 0.999)}
}

func countErrs(errs []error) int {
	n := 0
	for _, e := range errs {
		if e != nil {
			n++
		}
	}
	return n
}

func firstErr(errs []error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// quantile is the q-quantile of sorted (nearest rank).
func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q*float64(len(sorted))+0.5) - 1
	return sorted[max(0, min(i, len(sorted)-1))]
}

func median[T any](xs []T, f func(T) time.Duration) time.Duration {
	v := make([]time.Duration, len(xs))
	for i, x := range xs {
		v[i] = f(x)
	}
	slices.Sort(v)
	if len(v) == 0 {
		return 0
	}
	return v[len(v)/2]
}

// write saves results.json and, on GitHub Actions, a table in the job summary.
func (r loadResult) write(t *testing.T, path string) {
	t.Helper()
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil { //nolint:gosec // G301: a results directory
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil { //nolint:gosec // G306: results, not a secret
		t.Fatal(err)
	}
	summary := os.Getenv("GITHUB_STEP_SUMMARY")
	if summary == "" {
		return
	}
	var md strings.Builder
	fmt.Fprintf(&md, "### Load test (ADR-0024 §10)\n\nAdded latency, median of %d rounds: **p50 %s, p99 %s** (budget %s p99). %d req/s for %s per round, %d CPUs, %s.\n\n",
		len(r.Rounds), r.AddedP50, r.AddedP99, r.Budget, r.Rate, r.Round, r.CPUs, r.Go)
	md.WriteString("| Round | direct p50 | direct p99 | proxy p50 | proxy p99 | added p50 | added p99 |\n|---|---|---|---|---|---|---|\n")
	for i, x := range r.Rounds {
		fmt.Fprintf(&md, "| %d | %s | %s | %s | %s | %s | %s |\n", i+1, x.Direct.P50, x.Direct.P99, x.Proxy.P50, x.Proxy.P99, x.AddedP50, x.AddedP99)
	}
	f, err := os.OpenFile(summary, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644) //nolint:gosec // G302/G304: GitHub's summary file
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString(md.String()); err != nil {
		t.Fatal(err)
	}
}

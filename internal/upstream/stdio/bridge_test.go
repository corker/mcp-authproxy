//go:build linux || darwin

package stdio

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const userHeader = "X-Test-User" // the bridge harness's stand-in for the signed-in user

type bridgeHarness struct {
	*harness
	bridge *Bridge
	srv    *httptest.Server
}

// newBridge serves the fake upstream in its JSON-RPC mode through a Bridge. The user is taken
// from userHeader: "alice" or "bob".
func newBridge(t *testing.T, edit func(*Config)) *bridgeHarness {
	t.Helper()
	h := newHarness(t, func(c *Config) {
		c.Command = []string{fakeBin, "-mcp"}
		if edit != nil {
			edit(c)
		}
	})
	users := map[string]Identity{"alice": alice, "bob": bob}
	b := NewBridge(BridgeConfig{Manager: h.m, Log: slog.New(h.log), Identity: func(r *http.Request) (Identity, bool) {
		id, ok := users[r.Header.Get(userHeader)]
		return id, ok
	}})
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)
	return &bridgeHarness{harness: h, bridge: b, srv: srv}
}

// do sends one request; header holds extra name/value pairs, an empty value meaning none.
func (b *bridgeHarness) do(t *testing.T, ctx context.Context, method, user, session, body string, header ...string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, b.srv.URL, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json, text/event-stream")
	for i := 0; i+1 < len(header); i += 2 {
		if header[i+1] != "" { // an empty value sends no header at all
			req.Header.Set(header[i], header[i+1])
		}
	}
	if user != "" {
		req.Header.Set(userHeader, user)
	}
	if session != "" {
		req.Header.Set(sessionHeader, session)
	}
	res, err := b.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	t.Cleanup(func() { _ = res.Body.Close() })
	return res
}

func (b *bridgeHarness) post(t *testing.T, user, session, body string) *http.Response {
	t.Helper()
	return b.do(t, context.Background(), http.MethodPost, user, session, body)
}

const initialize = `{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}`

// initialize starts a session for user and returns its ID.
func (b *bridgeHarness) initialize(t *testing.T, user string) string {
	t.Helper()
	res := b.post(t, user, "", initialize)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("initialize: status %d", res.StatusCode)
	}
	events := readEvents(t, res)
	if len(events) != 1 || !strings.Contains(events[0], `"result":`) {
		t.Fatalf("initialize events = %q", events)
	}
	return res.Header.Get(sessionHeader)
}

// events reads SSE data lines one at a time.
type events struct {
	t        *testing.T
	ch       chan string
	comments atomic.Int64 // comment lines seen, which a client ignores: keep-alives
}

func eventsOf(t *testing.T, res *http.Response) *events {
	t.Helper()
	if ct := res.Header.Get("Content-Type"); ct != "text/event-stream" {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		t.Fatalf("Content-Type = %q, want text/event-stream; status %d, body %q", ct, res.StatusCode, body)
	}
	if v := res.Header.Get("X-Accel-Buffering"); v != "no" {
		t.Fatalf("X-Accel-Buffering = %q, want no: a buffering reverse proxy would hold the stream back", v)
	}
	e := &events{t: t, ch: make(chan string, 64)}
	go func() {
		defer close(e.ch)
		sc := bufio.NewScanner(res.Body)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), ":") {
				e.comments.Add(1) // counted before any later event is delivered
			} else if d, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
				e.ch <- d
			}
		}
	}()
	return e
}

// next returns the next event; ok is false once the stream has ended.
func (e *events) next() (data string, ok bool) {
	e.t.Helper()
	select {
	case d, ok := <-e.ch:
		return d, ok
	case <-time.After(guard):
		e.t.Fatal("no event")
		return "", false
	}
}

func (e *events) all() []string {
	var out []string
	for d, ok := e.next(); ok; d, ok = e.next() {
		out = append(out, d)
	}
	return out
}

func readEvents(t *testing.T, res *http.Response) []string {
	t.Helper()
	return eventsOf(t, res).all()
}

// eventually fails the test unless cond becomes true within guard.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(guard)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func rpcErrorCode(t *testing.T, body []byte) int {
	t.Helper()
	var m struct {
		Error struct{ Code int } `json:"error"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("not JSON-RPC: %q", body)
	}
	return m.Error.Code
}

func TestEachInitializeStartsItsOwnUpstreamProcessUnderAProxyIssuedMCPSessionID(t *testing.T) {
	b := newBridge(t, nil)
	s1, s2 := b.initialize(t, "alice"), b.initialize(t, "alice")
	for _, s := range []string{s1, s2} {
		if len(s) != 43 || strings.ContainsFunc(s, func(r rune) bool { return r < 0x21 || r > 0x7e }) {
			t.Fatalf("session ID %q: want 43 visible ASCII characters", s)
		}
	}
	if s1 == s2 || b.m.Len() != 2 {
		t.Fatalf("sessions %q, %q with %d processes; want two distinct sessions, two processes", s1, s2, b.m.Len())
	}
}

func TestPostWithoutAnMCPSessionOtherThanInitializeIs400AndStartsNoUpstreamProcess(t *testing.T) {
	b := newBridge(t, nil)
	for name, body := range map[string]string{
		"legacy request":               `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`,
		"older protocol version":       `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2025-06-18"}}}`,
		"notification":                 `{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		"initialize as a notification": `{"jsonrpc":"2.0","method":"initialize"}`,
	} {
		t.Run(name, func(t *testing.T) {
			res := b.post(t, "alice", "", body)
			raw, _ := io.ReadAll(res.Body)
			// Not a modern error code (-32020, -32022), so a dual-era client falls back to initialize.
			if res.StatusCode != http.StatusBadRequest || rpcErrorCode(t, raw) != codeInvalidRequest {
				t.Fatalf("status %d, body %s; want 400 with code %d", res.StatusCode, raw, codeInvalidRequest)
			}
		})
	}
	if b.m.Len() != 0 {
		t.Fatalf("Len = %d, want no process started", b.m.Len())
	}
}

func TestMalformedPostsAre400AndStartNothing(t *testing.T) {
	b := newBridge(t, nil)
	for name, tc := range map[string]struct {
		body string
		code int
	}{
		"batch":                            {`[` + initialize + `]`, codeInvalidRequest},
		"not JSON":                         {`{"jsonrpc":`, codeParseError},
		"not object":                       {`"initialize"`, codeParseError},
		"empty":                            {``, codeParseError},
		"no method":                        {`{"jsonrpc":"2.0","id":1}`, codeInvalidRequest},
		"null response id":                 {`{"jsonrpc":"2.0","id":null,"result":{}}`, codeInvalidRequest},
		"null request id":                  {`{"jsonrpc":"2.0","id":null,"method":"initialize"}`, codeInvalidRequest},
		"boolean id":                       {`{"jsonrpc":"2.0","id":true,"method":"initialize"}`, codeInvalidRequest},
		"fractional id":                    {`{"jsonrpc":"2.0","id":1.5,"method":"initialize"}`, codeInvalidRequest},
		"object id":                        {`{"jsonrpc":"2.0","id":{},"method":"initialize"}`, codeInvalidRequest},
		"method repeated, initialize last": {`{"jsonrpc":"2.0","id":1,"method":"tools/call","method":"initialize"}`, codeInvalidRequest},
		"method also in another case, initialize last": {`{"jsonrpc":"2.0","id":1,"method":"tools/call","METHOD":"initialize"}`, codeInvalidRequest},
		"another top-level key repeated":               {`{"jsonrpc":"2.0","jsonrpc":"2.0","id":1,"method":"initialize"}`, codeInvalidRequest},
	} {
		t.Run(name, func(t *testing.T) {
			res := b.post(t, "alice", "", tc.body)
			raw, _ := io.ReadAll(res.Body)
			if res.StatusCode != http.StatusBadRequest || rpcErrorCode(t, raw) != tc.code {
				t.Fatalf("status %d, body %s; want 400 with code %d", res.StatusCode, raw, tc.code)
			}
		})
	}
	if b.m.Len() != 0 {
		t.Fatalf("Len = %d, want 0", b.m.Len())
	}
}

func TestAmbiguousNotificationsAndResponsesInASessionAreRefused(t *testing.T) {
	// In a session these would be relayed (202) if the ambiguity were not caught for every kind.
	b := newBridge(t, nil)
	session := b.initialize(t, "alice")
	for name, body := range map[string]string{
		"notification with the method also in another case": `{"jsonrpc":"2.0","method":"notifications/x","METHOD":"notifications/y"}`,
		"response with a top-level key repeated":            `{"jsonrpc":"2.0","id":1,"result":{},"result":{"a":1}}`,
	} {
		t.Run(name, func(t *testing.T) {
			res := b.post(t, "alice", session, body)
			raw, _ := io.ReadAll(res.Body)
			if res.StatusCode != http.StatusBadRequest || rpcErrorCode(t, raw) != codeInvalidRequest || !strings.Contains(string(raw), "ambiguous") {
				t.Fatalf("status %d, body %s; want 400 -32600 naming the ambiguity", res.StatusCode, raw)
			}
		})
	}
}

func TestRequestWithoutAUserIsRefused(t *testing.T) {
	b := newBridge(t, nil)
	if res := b.post(t, "", "", initialize); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", res.StatusCode)
	}
}

func TestUnknownOrAnotherUsersMCPSessionIs404(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	echo := `{"jsonrpc":"2.0","id":1,"method":"echo","params":{}}`
	for _, tc := range []struct{ user, session string }{{"alice", "nope"}, {"bob", s}} {
		for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodDelete} {
			if res := b.do(t, context.Background(), method, tc.user, tc.session, echo); res.StatusCode != http.StatusNotFound {
				t.Errorf("%s by %s on %q: status %d, want 404", method, tc.user, tc.session, res.StatusCode)
			}
		}
	}
	// bob's DELETE must not have stopped alice's process: a stopping one would get 404 here.
	if got := readEvents(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":2,"method":"echo","params":{}}`)); len(got) != 1 {
		t.Fatalf("alice's own request after the others' attempts got %q", got)
	}
}

func TestRequestStreamCarriesItsRelatedMessagesThenItsResponse(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	got := readEvents(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":"p","method":"progress"}`))
	want := []string{
		`{"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":0}}`,
		`{"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":1}}`,
		`{"jsonrpc":"2.0","id":"p","result":"done"}`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("events =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestBodyIsRelayedUnchangedExceptLineBreaks(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	for name, tc := range map[string]struct{ body, line string }{
		"kept as sent": {` {"jsonrpc":"2.0", "id":1, "method":"line", "params":{"a": "é\\n", "b":1.50}} `,
			`{"jsonrpc":"2.0", "id":1, "method":"line", "params":{"a": "é\\n", "b":1.50}}`},
		"pretty-printed": {"{\n  \"jsonrpc\": \"2.0\",\r\n  \"id\": 2,\n  \"method\": \"line\"\n}",
			`{"jsonrpc":"2.0","id":2,"method":"line"}`},
	} {
		t.Run(name, func(t *testing.T) {
			got := readEvents(t, b.post(t, "alice", s, tc.body))
			var m struct{ Result string }
			if len(got) != 1 || json.Unmarshal([]byte(got[0]), &m) != nil || m.Result != tc.line {
				t.Fatalf("events %q: upstream received %q, want %q", got, m.Result, tc.line)
			}
		})
	}
}

func TestUpstreamLineThatIsNotAJSONRPCMessageIsDroppedAndLogged(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	held := eventsOf(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":"h","method":"hold"}`))
	b.post(t, "alice", s, `{"jsonrpc":"2.0","method":"x"}`) // keeps the order: hold is in before noresult
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := b.do(t, ctx, http.MethodPost, "alice", s, `{"jsonrpc":"2.0","id":"n","method":"noresult"}`)
	ev := eventsOf(t, res)
	eventually(t, "the invalid line to be dropped", func() bool {
		return len(b.log.with("upstream wrote a line that is not a JSON-RPC message")) == 1
	})
	cancel()
	if got := ev.all(); len(got) != 0 {
		t.Fatalf("the request the invalid line claims to answer got %q", got)
	}
	readEvents(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":"r","method":"release"}`))
	if got := held.all(); len(got) != 1 || got[0] != `{"jsonrpc":"2.0","id":"h","result":"released"}` {
		t.Fatalf("held request got %q: the invalid line must reach no stream", got)
	}
}

func TestUpstreamOutputWithABareCRStaysOneEvent(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	got := readEvents(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":1,"method":"raw"}`))
	if len(got) != 1 || got[0] != `{"jsonrpc":"2.0","id":1,"result":"raw"}` {
		t.Fatalf("events = %q", got)
	}
}

func TestUpstreamRequestReachesTheClientAndItsAnswerReachesTheUpstream(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	ev := eventsOf(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":"q","method":"ask"}`))
	if got, _ := ev.next(); got != `{"jsonrpc":"2.0","id":"srv-1","method":"sampling/createMessage","params":{}}` {
		t.Fatalf("first event %q, want the upstream's sampling request", got)
	}
	if res := b.post(t, "alice", s, `{"jsonrpc":"2.0","id":"srv-1","result":{"answer":42}}`); res.StatusCode != http.StatusAccepted {
		t.Fatalf("client response: status %d, want 202", res.StatusCode)
	}
	if got := ev.all(); len(got) != 1 || got[0] != `{"jsonrpc":"2.0","id":"q","result":{"answer":42}}` {
		t.Fatalf("then %q, want the result carrying the client's answer", got)
	}
}

func TestClientNotificationIsAcceptedWith202AndNoBody(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	res := b.post(t, "alice", s, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	if body, _ := io.ReadAll(res.Body); res.StatusCode != http.StatusAccepted || len(body) != 0 {
		t.Fatalf("status %d, body %q; want 202 with no body", res.StatusCode, body)
	}
}

// last asks the fake upstream for the last client response it could not match.
func (b *bridgeHarness) last(t *testing.T, session string) string {
	t.Helper()
	got := readEvents(t, b.post(t, "alice", session, `{"jsonrpc":"2.0","id":"last","method":"last"}`))
	if len(got) != 1 {
		t.Fatalf("last: %q", got)
	}
	var m struct{ Result json.RawMessage }
	_ = json.Unmarshal([]byte(got[0]), &m)
	return string(m.Result)
}

func TestUpstreamRequestWithNoClientStreamIsAnsweredWithAnErrorAndLogged(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	readEvents(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":1,"method":"asklater"}`))
	// No HTTP request until the upstream's request has been routed: an open one would carry it.
	eventually(t, "the undeliverable request to be logged", func() bool {
		return len(b.log.with("upstream request answered with an error: no client stream")) == 1
	})
	var got string
	eventually(t, "the upstream to receive the error", func() bool { got = b.last(t, s); return got != "null" })
	if !strings.Contains(got, `"id":"srv-later"`) || !strings.Contains(got, `"code":-32603`) || !strings.Contains(got, "client not connected") {
		t.Fatalf("upstream got %s, want a -32603 error for srv-later", got)
	}
}

func (b *bridgeHarness) listen(t *testing.T, ctx context.Context, session string) *events {
	t.Helper()
	res := b.do(t, ctx, http.MethodGet, "alice", session, "")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET: status %d", res.StatusCode)
	}
	return eventsOf(t, res)
}

func TestMessagesOutsideARequestGoToTheNewestGETStream(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	first := b.listen(t, context.Background(), s)
	second := b.listen(t, context.Background(), s)
	if got := first.all(); len(got) != 0 {
		t.Fatalf("the replaced GET stream got %q", got)
	}
	readEvents(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":1,"method":"announce"}`))
	if got, _ := second.next(); got != `{"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info"}}` {
		t.Fatalf("GET stream got %q", got)
	}
}

func TestNotificationsWithoutAStreamAreQueuedInOrderDroppingTheOldestPastTheLimit(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	n := maxQueued + 10
	readEvents(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":1,"method":"floodlater","params":{"n":`+itoa(n)+`}}`))
	eventually(t, "the 10 oldest notifications to be dropped", func() bool {
		return len(b.log.with("upstream notification dropped")) == 10
	})
	ev := b.listen(t, context.Background(), s)
	for i := 10; i < n; i++ {
		want := `{"jsonrpc":"2.0","method":"notifications/message","params":{"i":` + itoa(i) + `}}`
		if got, _ := ev.next(); got != want {
			t.Fatalf("GET event for i=%d is %s, want %s", i, got, want)
		}
	}
}

func TestNewNotificationNeverOvertakesQueuedOnes(t *testing.T) {
	// White-box: a GET stream whose buffer filled while notifications were still queued.
	s := &session{log: slog.New(slog.DiscardHandler), byID: map[string]*stream{}}
	for i := range streamBuffer + 1 {
		s.route([]byte(`{"jsonrpc":"2.0","method":"n","params":{"i":` + itoa(i) + `}}`))
	}
	st := newStream(nil)
	s.setListen(st)
	s.route([]byte(`{"jsonrpc":"2.0","method":"late"}`))
	var got []string
	for range streamBuffer + 2 {
		got = append(got, string(<-st.out))
		s.refill(st)
	}
	queued := `{"jsonrpc":"2.0","method":"n","params":{"i":` + itoa(streamBuffer) + `}}`
	if got[streamBuffer] != queued || got[streamBuffer+1] != `{"jsonrpc":"2.0","method":"late"}` {
		t.Fatalf("delivered %q last, want %s then late", got[streamBuffer:], queued)
	}
}

func TestFullStreamDropsNotificationsAndAnswersUpstreamRequestsWithAnErrorAtOnce(t *testing.T) {
	// White-box: a client stream whose buffer is full, on a real upstream process.
	h := newHarness(t, func(c *Config) { c.Command = []string{fakeBin, "-mcp"} })
	p := h.get(t, "s1", alice)
	s := &session{p: p, log: slog.New(h.log), byID: map[string]*stream{}}
	st := newStream(nil)
	for range streamBuffer {
		st.out <- []byte(`{}`)
	}
	s.setListen(st)
	s.route([]byte(`{"jsonrpc":"2.0","method":"notifications/message"}`))
	s.route([]byte(`{"jsonrpc":"2.0","id":"srv-x","method":"roots/list"}`))
	if len(h.log.with("upstream notification dropped")) != 1 {
		t.Fatal("the notification for the full stream was not dropped")
	}
	var got string
	eventually(t, "the upstream to receive the error", func() bool {
		got = call(t, p, `{"jsonrpc":"2.0","id":"last","method":"last"}`)
		return !strings.Contains(got, `"result":null`)
	})
	if !strings.Contains(got, `"id":"srv-x"`) || !strings.Contains(got, "client not connected") {
		t.Fatalf("upstream got %s, want an error for srv-x", got)
	}
}

func itoa(i int) string { b, _ := json.Marshal(i); return string(b) }

func TestDeleteStopsTheUpstreamProcessAndEndsTheMCPSession(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	if res := b.do(t, context.Background(), http.MethodDelete, "alice", s, ""); res.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: status %d, want 204", res.StatusCode)
	}
	eventually(t, "the process to exit after DELETE", func() bool { return b.m.Len() == 0 })
	if res := b.post(t, "alice", s, `{"jsonrpc":"2.0","id":1,"method":"echo"}`); res.StatusCode != http.StatusNotFound {
		t.Fatalf("POST after DELETE: status %d, want 404", res.StatusCode)
	}
}

func TestCrashFailsInFlightRequestsAndLaterOnesGet404(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	held := eventsOf(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":"h","method":"hold"}`))
	crash := eventsOf(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":"c","method":"crash"}`))
	for name, ev := range map[string]*events{"held": held, "crashing": crash} {
		got := ev.all()
		if len(got) != 1 || !strings.Contains(got[0], `"code":-32603`) || !strings.Contains(got[0], "upstream process exited") {
			t.Fatalf("%s request got %q, want a -32603 error", name, got)
		}
	}
	deadline := time.Now().Add(guard)
	for {
		res := b.post(t, "alice", s, `{"jsonrpc":"2.0","id":1,"method":"echo"}`)
		if res.StatusCode == http.StatusNotFound {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("POST after the crash: status %d, want 404", res.StatusCode)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestClientDisconnectFreesTheRequestIDAndDropsTheLateResponse(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	ctx, cancel := context.WithCancel(context.Background())
	res := b.do(t, ctx, http.MethodPost, "alice", s, `{"jsonrpc":"2.0","id":"h","method":"hold"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d", res.StatusCode)
	}
	cancel()
	// The id is free again once the route is gone; until then it is a duplicate.
	deadline := time.Now().Add(guard)
	for {
		res := b.post(t, "alice", s, `{"jsonrpc":"2.0","id":"h","method":"echo","params":{}}`)
		if res.StatusCode == http.StatusOK {
			readEvents(t, res)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("status %d: the disconnected request's route was never dropped", res.StatusCode)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := readEvents(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":"r","method":"release"}`)); len(got) != 1 || got[0] != `{"jsonrpc":"2.0","id":"r","result":"ok"}` {
		t.Fatalf("release got %q", got)
	}
	// The held request's response came first, for nobody: it was dropped and logged.
	if len(b.log.with("upstream response for no waiting request")) != 1 {
		t.Fatal("the late response was not logged as unrouted")
	}
}

func TestDuplicateInFlightRequestIDIs400AndLeavesTheFirstRequestRunning(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	held := eventsOf(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":7,"method":"hold"}`))
	res := b.post(t, "alice", s, `{"jsonrpc":"2.0","id":7,"method":"echo"}`)
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusBadRequest || rpcErrorCode(t, raw) != codeInvalidRequest {
		t.Fatalf("status %d, body %s; want 400", res.StatusCode, raw)
	}
	readEvents(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":8,"method":"release"}`))
	if got := held.all(); len(got) != 1 || got[0] != `{"jsonrpc":"2.0","id":7,"result":"released"}` {
		t.Fatalf("held request got %q", got)
	}
}

func TestUserAtTheirCapWithEveryProcessInUseGets503WhileOthersStillStart(t *testing.T) {
	b := newBridge(t, func(c *Config) { c.MaxPerUser = 1 })
	s := b.initialize(t, "alice")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.listen(t, ctx, s) // an open GET stream keeps the process in use
	res := b.post(t, "alice", "", initialize)
	if res.StatusCode != http.StatusServiceUnavailable || res.Header.Get("Retry-After") != busyRetryAfter {
		t.Fatalf("status %d, Retry-After %q; want 503 and %s", res.StatusCode, res.Header.Get("Retry-After"), busyRetryAfter)
	}
	if b.post(t, "bob", "", initialize).StatusCode != http.StatusOK {
		t.Fatal("bob is under his own cap and must get a session")
	}
}

func TestInitializeThatCannotStartOrReachTheUpstreamProcessIs502WithoutAnMCPSession(t *testing.T) {
	for name, tc := range map[string]struct {
		command []string // nil: the fake upstream in its JSON-RPC mode
		start   func(*exec.Cmd) error
	}{
		"start fails": {start: func(*exec.Cmd) error { return errors.New("exec failed") }},
		"stdin closed": {start: func(c *exec.Cmd) error {
			c.Stdin = nil // the process reads /dev/null; nothing holds the pipe's read end, so the write fails
			return c.Start()
		}},
		"exits before answering": {command: []string{fakeBin, "-mcp", "-init=exit"}},
	} {
		t.Run(name, func(t *testing.T) {
			b := newBridge(t, func(c *Config) {
				if tc.command != nil {
					c.Command = tc.command
				}
			})
			if tc.start != nil {
				b.m.start = tc.start
			}
			res := b.post(t, "alice", "", initialize)
			// A 404 would read as a legacy HTTP+SSE server to a dual-era client.
			if res.StatusCode != http.StatusBadGateway || res.Header.Get(sessionHeader) != "" {
				t.Fatalf("status %d, session %q; want 502 and no session ID", res.StatusCode, res.Header.Get(sessionHeader))
			}
			eventually(t, "no upstream process left", func() bool { return b.m.Len() == 0 })
		})
	}
}

func TestOpenGETStreamEndsWhenTheUpstreamProcessExits(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	ev := b.listen(t, context.Background(), s)
	readEvents(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":"c","method":"crash"}`))
	if got := ev.all(); len(got) != 0 { // all returns once the stream has ended
		t.Fatalf("GET stream got %q", got)
	}
	eventually(t, "the MCP session to end", func() bool {
		return b.do(t, context.Background(), http.MethodGet, "alice", s, "").StatusCode == http.StatusNotFound
	})
}

func TestStoppingMCPSessionIs404ForEveryMethod(t *testing.T) {
	// A stubborn upstream keeps running after DELETE until the kill timers, which the fake clock
	// never fires: the session is stopping for the whole test.
	b := newBridge(t, func(c *Config) { c.Command = []string{fakeBin, "-mcp", "-stubborn"} })
	s := b.initialize(t, "alice")
	if res := b.do(t, context.Background(), http.MethodDelete, "alice", s, ""); res.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: status %d", res.StatusCode)
	}
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		if res := b.do(t, context.Background(), method, "alice", s, `{"jsonrpc":"2.0","id":1,"method":"echo","params":{}}`); res.StatusCode != http.StatusNotFound {
			t.Errorf("%s on the stopping session: status %d, want 404", method, res.StatusCode)
		}
	}
	if b.m.Len() != 1 {
		t.Fatalf("Len = %d: the test needs the process still stopping", b.m.Len())
	}
}

func TestMissingOrDuplicateMCPSessionHeadersAndUnsupportedMethodsStartNoProcess(t *testing.T) {
	b := newBridge(t, nil)
	s := b.initialize(t, "alice")
	for name, tc := range map[string]struct {
		method   string
		sessions []string
		want     int
		allow    string // the Allow header expected; only a 405 lists the methods
	}{
		"two MCP session headers":                      {http.MethodPost, []string{s, s}, http.StatusBadRequest, ""},
		"GET without an MCP session is not allowed":    {http.MethodGet, nil, http.StatusMethodNotAllowed, "POST"},
		"DELETE without an MCP session is not allowed": {http.MethodDelete, nil, http.StatusMethodNotAllowed, "POST"},
		"PUT": {http.MethodPut, []string{s}, http.StatusMethodNotAllowed, "GET, POST, DELETE"},
	} {
		t.Run(name, func(t *testing.T) {
			req, _ := http.NewRequest(tc.method, b.srv.URL, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"echo","params":{}}`))
			req.Header.Set(userHeader, "alice")
			for _, v := range tc.sessions {
				req.Header.Add(sessionHeader, v)
			}
			res, err := b.srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = res.Body.Close()
			if res.StatusCode != tc.want || res.Header.Get("Allow") != tc.allow {
				t.Fatalf("status %d, Allow %q; want %d, %q", res.StatusCode, res.Header.Get("Allow"), tc.want, tc.allow)
			}
		})
	}
	if b.m.Len() != 1 {
		t.Fatalf("Len = %d, want only the one session's process", b.m.Len())
	}
}

func TestBodyOverTheLimitIs413AndStartsNoProcess(t *testing.T) {
	b := newBridge(t, nil)
	limited := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 64) // as the server's body limit does
		b.bridge.ServeHTTP(w, r)
	}))
	t.Cleanup(limited.Close)
	req, _ := http.NewRequest(http.MethodPost, limited.URL, strings.NewReader(initialize))
	req.Header.Set(userHeader, "alice")
	res, err := limited.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusRequestEntityTooLarge || b.m.Len() != 0 {
		t.Fatalf("status %d with %d processes, want 413 and none", res.StatusCode, b.m.Len())
	}
}

func TestInitializeTheUpstreamFailsGetsNoMCPSessionAndStopsTheProcess(t *testing.T) {
	t.Run("error answer", func(t *testing.T) {
		b := newBridge(t, func(c *Config) { c.Command = []string{fakeBin, "-mcp", "-init=error"} })
		res := b.post(t, "alice", "", initialize)
		body, _ := io.ReadAll(res.Body)
		// The upstream's own answer reaches the client, as JSON, with no session ID.
		if res.StatusCode != http.StatusOK || res.Header.Get(sessionHeader) != "" || rpcErrorCode(t, body) != -32602 {
			t.Fatalf("status %d, session %q, body %s; want the upstream's error and no session ID", res.StatusCode, res.Header.Get(sessionHeader), body)
		}
		eventually(t, "no upstream process left", func() bool { return b.m.Len() == 0 })
	})
	t.Run("error after a notification", func(t *testing.T) {
		// The stream has started, with the session ID, before the error: the process still stops.
		b := newBridge(t, func(c *Config) { c.Command = []string{fakeBin, "-mcp", "-init=lognerror"} })
		res := b.post(t, "alice", "", initialize)
		if got := readEvents(t, res); len(got) != 2 || !strings.Contains(got[1], "-32602") {
			t.Fatalf("events %q, want the notification then the upstream's error", got)
		}
		eventually(t, "no upstream process left", func() bool { return b.m.Len() == 0 })
		// The ID that went out leads nowhere: no MCP session.
		if got := b.do(t, context.Background(), http.MethodGet, "alice", res.Header.Get(sessionHeader), "").StatusCode; got != http.StatusNotFound {
			t.Fatalf("GET with the issued ID: status %d, want 404", got)
		}
	})
	t.Run("client leaves after a notification", func(t *testing.T) {
		b := newBridge(t, func(c *Config) { c.Command = []string{fakeBin, "-mcp", "-init=lognhold"} })
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ev := eventsOf(t, b.do(t, ctx, http.MethodPost, "alice", "", initialize))
		if got, _ := ev.next(); !strings.Contains(got, "notifications/message") {
			t.Fatalf("first event %q, want the upstream's notification", got)
		}
		cancel()
		eventually(t, "the unusable process to be stopped", func() bool { return b.m.Len() == 0 })
	})
	t.Run("client leaves before the answer", func(t *testing.T) {
		b := newBridge(t, func(c *Config) { c.Command = []string{fakeBin, "-mcp", "-init=hold"} })
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, b.srv.URL, strings.NewReader(initialize))
		req.Header.Set(userHeader, "alice")
		done := make(chan struct{})
		go func() {
			defer close(done)
			if res, err := b.srv.Client().Do(req); err == nil {
				_ = res.Body.Close()
			}
		}()
		eventually(t, "the process to start", func() bool { return b.m.Len() == 1 })
		cancel()
		<-done
		// Stopping closes stdin; the fake then exits, and nobody holds a session for it.
		eventually(t, "the unusable process to be stopped", func() bool { return b.m.Len() == 0 })
	})
}

func TestDeleteEndsTheMCPSessionsOpenStreamsAtOnce(t *testing.T) {
	// A stubborn upstream keeps running after DELETE until the kill timers, which the fake clock
	// never fires: only DELETE itself can end the streams.
	b := newBridge(t, func(c *Config) { c.Command = []string{fakeBin, "-mcp", "-stubborn"} })
	s := b.initialize(t, "alice")
	get := b.listen(t, context.Background(), s)
	held := eventsOf(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":"h","method":"hold"}`))
	if res := b.do(t, context.Background(), http.MethodDelete, "alice", s, ""); res.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: status %d", res.StatusCode)
	}
	if got := get.all(); len(got) != 0 {
		t.Fatalf("GET stream got %q", got)
	}
	if got := held.all(); len(got) != 1 || !strings.Contains(got[0], "-32603") {
		t.Fatalf("held request got %q, want a -32603 error", got)
	}
}

func TestUpstreamProcessCrashingDuringInitializeIsLoggedAsACrash(t *testing.T) {
	b := newBridge(t, func(c *Config) { c.Command = []string{fakeBin, "-mcp", "-init=exit"} })
	if res := b.post(t, "alice", "", initialize); res.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", res.StatusCode)
	}
	eventually(t, "the process to be gone", func() bool { return b.m.Len() == 0 })
	if len(b.log.with("stopping upstream process")) != 0 || len(b.log.with("upstream process exited unexpectedly")) != 1 {
		t.Fatal("a process that crashed must be logged as an unexpected exit, not as a stop")
	}
}

func TestProtocolVersionHeaderOtherThanTheMCPSessionsNegotiatedVersionIs400(t *testing.T) {
	for name, tc := range map[string]struct {
		negotiated string // the version the upstream's InitializeResult names; "none" leaves it out
		header     string // "" sends none
		rejected   bool
	}{
		"absent":             {"2025-11-25", "", false},
		"the negotiated one": {"2025-11-25", "2025-11-25", false},
		"another version":    {"2025-11-25", "2025-03-26", true},
		"not a version":      {"2025-11-25", "latest", true},
		"the negotiated one when the upstream chose an older version": {"2025-06-18", "2025-06-18", false},
		"a newer version than the upstream negotiated":                {"2025-06-18", "2025-11-25", true},
		"any header when the upstream named no version":               {"none", "2025-11-25", false},
	} {
		t.Run(name, func(t *testing.T) {
			b := newBridge(t, func(c *Config) { c.Command = []string{fakeBin, "-mcp", "-version=" + tc.negotiated} })
			s := b.initialize(t, "alice")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel() // ends the GET stream
			for _, m := range []struct {
				method string
				ok     int
			}{{http.MethodPost, http.StatusOK}, {http.MethodGet, http.StatusOK}, {http.MethodDelete, http.StatusNoContent}} {
				res := b.do(t, ctx, m.method, "alice", s, `{"jsonrpc":"2.0","id":1,"method":"echo","params":{}}`, "MCP-Protocol-Version", tc.header)
				want := m.ok
				if tc.rejected {
					want = http.StatusBadRequest
				}
				if res.StatusCode != want {
					t.Fatalf("%s: status %d, want %d", m.method, res.StatusCode, want)
				}
				if tc.rejected {
					body, _ := io.ReadAll(res.Body)
					if rpcErrorCode(t, body) != codeInvalidRequest || !strings.Contains(string(body), `"id":null`) {
						t.Fatalf("%s: body %s, want a -32600 error with no id", m.method, body)
					}
				}
			}
			if tc.rejected { // the rejected DELETE left the MCP session as it was
				if got := readEvents(t, b.post(t, "alice", s, `{"jsonrpc":"2.0","id":2,"method":"echo","params":{}}`)); len(got) != 1 {
					t.Fatalf("the MCP session after rejected requests got %q", got)
				}
			}
		})
	}
}

func TestRejectedProtocolVersionLeavesTheProcessIdle(t *testing.T) {
	// At a per-user cap of one, a new MCP session can start only by evicting the idle process: a
	// rejected request that kept its lease would make that a 503.
	b := newBridge(t, func(c *Config) { c.MaxPerUser = 1 })
	s := b.initialize(t, "alice")
	if res := b.do(t, context.Background(), http.MethodPost, "alice", s, `{"jsonrpc":"2.0","id":1,"method":"echo","params":{}}`,
		"MCP-Protocol-Version", "2025-03-26"); res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", res.StatusCode)
	}
	if got := b.post(t, "alice", "", initialize).StatusCode; got != http.StatusOK {
		t.Fatalf("a new MCP session at the cap: status %d, want 200", got)
	}
}

//go:build linux || darwin

package stdio

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAKeepAliveIsAnSSECommentLine(t *testing.T) {
	rec := httptest.NewRecorder()
	if err := startSSE(rec).keepAlive(); err != nil {
		t.Fatal(err)
	}
	if got := rec.Body.String(); got != ":\r\n" {
		t.Fatalf("keep-alive wrote %q, want %q", got, ":\r\n")
	}
}

func TestQuietSignalsAfterTheIntervalAndAWriteDropsADueSignal(t *testing.T) {
	ready := func(q *quiet) bool {
		select {
		case <-q.elapsed():
			return true
		default:
			return false
		}
	}
	clk := newFakeClock()
	q := newQuiet(clk)
	clk.Advance(keepAliveInterval - time.Nanosecond)
	if ready(q) {
		t.Fatal("signalled before the interval")
	}
	clk.Advance(time.Nanosecond) // fires while nobody is receiving: the signal must be kept
	if !ready(q) {
		t.Fatal("no signal after the interval")
	}
	clk.Advance(keepAliveInterval) // that signal was taken: nothing re-arms the timer until a write
	if ready(q) {
		t.Fatal("re-armed itself without a write")
	}
	q.writing()
	clk.Advance(keepAliveInterval - time.Nanosecond)
	q.writing()
	if ready(q) {
		t.Fatal("signalled while writes came within the interval")
	}
	clk.Advance(keepAliveInterval)
	q.writing() // a signal already due is dropped by a write
	if ready(q) {
		t.Fatal("a signal already due survived a write")
	}
	clk.Advance(keepAliveInterval)
	if !ready(q) {
		t.Fatal("no signal a full interval after the write")
	}
	q.writing() // armed again
	q.stop()
	clk.Advance(keepAliveInterval)
	if ready(q) {
		t.Fatal("signalled after stop")
	}
	var none *quiet // a stream that needs no keep-alive
	none.writing()
	none.stop()
	if none.elapsed() != nil {
		t.Fatal("a nil quiet signals")
	}
}

// longLived opens one kind of long-lived stream; push makes the upstream write one event on it and
// waits for it, which also proves every comment written before it has been counted.
type longLived func(t *testing.T) (b *bridgeHarness, ev *events, push func())

var longLivedStreams = map[string]longLived{
	"modern listen": func(t *testing.T) (*bridgeHarness, *events, func()) {
		b := newModernBridge(t, nil)
		ev := b.listenModern(t, context.Background(), `"L"`)
		return b, ev, func() {
			t.Helper()
			readEvents(t, b.modern(t, "alice", modernBody(`1`, "notify", "", "")))
			if got, _ := ev.next(); !strings.Contains(got, "notifications/tools/list_changed") {
				t.Fatalf("event %s, want the list_changed", got)
			}
		}
	},
	"legacy GET": func(t *testing.T) (*bridgeHarness, *events, func()) {
		b := newBridge(t, nil)
		session := b.initialize(t, "alice")
		ev := b.listen(t, context.Background(), session)
		push := func() {
			t.Helper()
			readEvents(t, b.post(t, "alice", session, `{"jsonrpc":"2.0","id":1,"method":"announce"}`))
			if got, _ := ev.next(); !strings.Contains(got, "notifications/message") {
				t.Fatalf("GET stream got %q, want the announcement", got)
			}
		}
		return b, ev, push // its headers came after its quiet timer was armed
	},
}

func TestAQuietLongLivedStreamGetsAKeepAliveEachIntervalAndAnActiveOneNone(t *testing.T) {
	for name, open := range longLivedStreams {
		t.Run(name, func(t *testing.T) {
			b, ev, push := open(t)
			// 15 s: under the 60 s idle cut-off of nginx proxy_read_timeout and AWS ALBs (#155).
			b.clk.Advance(15*time.Second - time.Second)
			push() // a write restarts the quiet period
			b.clk.Advance(time.Second)
			push()
			if n := ev.comments.Load(); n != 0 {
				t.Fatalf("%d keep-alives on a stream that wrote within the interval", n)
			}
			for want := int64(1); want <= 2; want++ {
				b.clk.Advance(15 * time.Second)
				eventually(t, "a keep-alive on the quiet stream", func() bool { return ev.comments.Load() >= want })
			}
			push()
			if n := ev.comments.Load(); n != 2 {
				t.Fatalf("%d keep-alives after two quiet intervals, want 2", n)
			}
		})
	}
}

func TestAModernRequestThatIsNotAListenGetsNoKeepAlive(t *testing.T) {
	b := newModernBridge(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The headers arrive once the stream has started, after any quiet timer is armed.
	ev := eventsOf(t, b.modernCtx(t, ctx, "alice", modernBody(`1`, "hold", "", "")))
	b.clk.Advance(2 * keepAliveInterval)
	time.Sleep(100 * time.Millisecond) // a keep-alive would be written within this; none may come
	if n := ev.comments.Load(); n != 0 {
		t.Fatalf("%d keep-alives on a tools-style request stream; only a listen is long-lived", n)
	}
}

// brokenWriter is a ResponseWriter whose writes fail once broken is set, as when the client has gone
// but the request's context has not ended.
type brokenWriter struct {
	header  http.Header
	mu      sync.Mutex
	body    bytes.Buffer
	broken  bool
	started bool // WriteHeader was called
}

func (w *brokenWriter) Header() http.Header { return w.header }
func (w *brokenWriter) Flush()              {}

func (w *brokenWriter) WriteHeader(int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.started = true
}

func (w *brokenWriter) hasStarted() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.started
}

func (w *brokenWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.broken {
		return 0, http.ErrHandlerTimeout
	}
	return w.body.Write(p)
}

func (w *brokenWriter) written() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.String()
}

func TestAKeepAliveThatCannotBeWrittenCancelsTheListen(t *testing.T) {
	b := newModernBridge(t, nil)
	body := modernBody(`"L"`, "subscriptions/listen", "", `"notifications":{"toolsListChanged":true}`)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)) // its context never ends
	req.Header.Set(userHeader, "alice")
	req.Header.Set("Accept", "application/json, text/event-stream")
	for h := modernHeaders(body); len(h) >= 2; h = h[2:] {
		req.Header.Set(h[0], h[1])
	}
	w := &brokenWriter{header: http.Header{}}
	done := make(chan struct{})
	go func() { defer close(done); b.bridge.ServeHTTP(w, req) }()
	eventually(t, "the acknowledgement", func() bool { return strings.Contains(w.written(), "acknowledged") })
	w.mu.Lock()
	w.broken = true
	w.mu.Unlock()
	b.clk.Advance(keepAliveInterval)
	select {
	case <-done:
	case <-time.After(guard):
		t.Fatal("the listen went on after its keep-alive could not be written")
	}
	if ids := b.awaitUpstreamCancellation(t); len(ids) != 1 {
		t.Fatalf("upstream saw cancellations %q, want the listen's", ids)
	}
}

func TestAKeepAliveThatCannotBeWrittenEndsTheLegacyGETStream(t *testing.T) {
	b := newBridge(t, nil)
	session := b.initialize(t, "alice")
	req := httptest.NewRequest(http.MethodGet, "/", nil) // its context never ends
	req.Header.Set(userHeader, "alice")
	req.Header.Set(sessionHeader, session)
	req.Header.Set("Accept", "text/event-stream")
	w := &brokenWriter{header: http.Header{}, broken: true}
	done := make(chan struct{})
	go func() { defer close(done); b.bridge.ServeHTTP(w, req) }()
	eventually(t, "the GET stream to start", w.hasStarted) // its quiet period starts before it
	b.clk.Advance(keepAliveInterval)
	select {
	case <-done:
	case <-time.After(guard):
		t.Fatal("the GET stream went on after its keep-alive could not be written")
	}
}

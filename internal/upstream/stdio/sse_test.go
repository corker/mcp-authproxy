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

func TestAQuietListenStreamGetsAKeepAliveEachIntervalAndAnActiveOneNone(t *testing.T) {
	b := newModernBridge(t, nil)
	ev := b.listenModern(t, context.Background(), `"L"`)
	notify := func() {
		t.Helper()
		readEvents(t, b.modern(t, "alice", modernBody(`1`, "notify", "", "")))
		if got, _ := ev.next(); !strings.Contains(got, "notifications/tools/list_changed") {
			t.Fatalf("event %s, want the list_changed", got)
		}
	}
	b.clk.Advance(keepAliveInterval - time.Second)
	notify() // a write restarts the quiet period
	b.clk.Advance(time.Second)
	notify() // a barrier: a comment written before it has been counted
	if n := ev.comments.Load(); n != 0 {
		t.Fatalf("%d keep-alives on a stream that wrote within the interval", n)
	}
	for want := int64(1); want <= 2; want++ {
		b.clk.Advance(keepAliveInterval)
		eventually(t, "a keep-alive on the quiet stream", func() bool { return ev.comments.Load() >= want })
	}
	notify()
	if n := ev.comments.Load(); n != 2 {
		t.Fatalf("%d keep-alives after two quiet intervals, want 2", n)
	}
}

func TestAQuietLegacyGETStreamGetsAKeepAlive(t *testing.T) {
	b := newBridge(t, nil)
	session := b.initialize(t, "alice")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ev := eventsOf(t, b.do(t, ctx, http.MethodGet, "alice", session, ""))
	eventually(t, "the GET stream to open", func() bool {
		b.bridge.mu.Lock()
		s := b.bridge.sessions[session]
		b.bridge.mu.Unlock()
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.getStream != nil
	})
	b.clk.Advance(keepAliveInterval)
	eventually(t, "a keep-alive on the quiet GET stream", func() bool { return ev.comments.Load() == 1 })
}

// brokenWriter is a ResponseWriter whose writes fail once broken is set, as when the client has gone
// but the request's context has not ended.
type brokenWriter struct {
	header http.Header
	mu     sync.Mutex
	body   bytes.Buffer
	broken bool
}

func (w *brokenWriter) Header() http.Header { return w.header }
func (w *brokenWriter) WriteHeader(int)     {}
func (w *brokenWriter) Flush()              {}

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

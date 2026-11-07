//go:build linux || darwin

package stdio

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"
)

var (
	eventPrefix = []byte("event: message\ndata: ")
	eventEnd    = []byte("\n\n")
	comment     = []byte(":\r\n") // an SSE comment line: clients ignore it (WHATWG HTML, server-sent events)
)

// keepAliveInterval is how long a long-lived stream may stay quiet before a comment line goes out,
// as 2026-07-28 streamable-http encourages for subscriptions/listen. It is under the idle cut-offs
// of common intermediaries: nginx proxy_read_timeout and an AWS ALB's idle timeout (60 s by
// default), Cloudflare's proxy read timeout (125 s).
const keepAliveInterval = 15 * time.Second

// sse writes server-sent events, each flushed and bounded by writeTimeout.
type sse struct {
	w  http.ResponseWriter
	rc *http.ResponseController
}

func startSSE(w http.ResponseWriter) sse {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no") // reverse proxies must not buffer a stream (2026-07-28 streamable-http)
	w.WriteHeader(http.StatusOK)
	s := sse{w: w, rc: http.NewResponseController(w)}
	_ = s.rc.Flush()
	return s
}

// keepAlive writes a comment line, which carries no event: it keeps a quiet stream open.
func (s sse) keepAlive() error {
	_ = s.rc.SetWriteDeadline(time.Now().Add(writeTimeout))
	if _, err := s.w.Write(comment); err != nil {
		return err
	}
	return s.rc.Flush()
}

// quiet signals once a stream has written nothing for keepAliveInterval. A nil *quiet never
// signals, for a stream that needs no keep-alive.
type quiet struct {
	t Timer
	c chan struct{}
}

func newQuiet(c Clock) *quiet {
	q := &quiet{c: make(chan struct{}, 1)}
	q.t = c.AfterFunc(keepAliveInterval, func() {
		select {
		case q.c <- struct{}{}:
		default:
		}
	})
	return q
}

// elapsed is ready once the stream has been quiet for keepAliveInterval.
func (q *quiet) elapsed() <-chan struct{} {
	if q == nil {
		return nil
	}
	return q.c
}

// writing restarts the quiet period; call it before each write, so whoever reads the write sees
// the period already restarted. A signal already due is dropped.
func (q *quiet) writing() {
	if q == nil {
		return
	}
	q.t.Reset(keepAliveInterval)
	select {
	case <-q.c:
	default:
	}
}

func (q *quiet) stop() {
	if q != nil {
		q.t.Stop()
	}
}

// write sends msg, one JSON-RPC message, as one event. A bare CR ends an SSE line, and in JSON it
// can only be whitespace, so a message holding one is compacted first.
func (s sse) write(msg []byte) error {
	if bytes.IndexByte(msg, '\r') >= 0 {
		var b bytes.Buffer
		if json.Compact(&b, msg) == nil {
			msg = b.Bytes()
		}
	}
	_ = s.rc.SetWriteDeadline(time.Now().Add(writeTimeout))
	for _, part := range [][]byte{eventPrefix, msg, eventEnd} { // buffered by net/http; no copy of msg
		if _, err := s.w.Write(part); err != nil {
			return err
		}
	}
	return s.rc.Flush()
}

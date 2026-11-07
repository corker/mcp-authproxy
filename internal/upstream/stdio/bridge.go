//go:build linux || darwin

package stdio

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/byward-dev/byward/internal/logging"
)

// sessionHeader is the MCP session header (2025-11-25 transports, Session Management).
const sessionHeader = "Mcp-Session-Id"

const (
	// streamBuffer is how many upstream messages wait for one slow HTTP stream; past it a
	// notification is dropped and a request is answered with an error.
	streamBuffer = 64
	// maxQueued notifications wait for the client to open a stream; past it the oldest is dropped.
	maxQueued = 256
	// writeTimeout bounds each write to an HTTP stream.
	writeTimeout = 30 * time.Second
	// busyRetryAfter is the Retry-After, in seconds, when a process cap is full.
	busyRetryAfter = "10"
)

// BridgeConfig is what NewBridge needs.
type BridgeConfig struct {
	Manager *Manager
	// Identity is the signed-in user of a request; false when there is none.
	Identity func(*http.Request) (Identity, bool)
	Log      *slog.Logger
}

// Bridge serves a stdio upstream as an MCP Streamable HTTP endpoint for the legacy era (2025-11-25):
// each `initialize` starts a process for a new session, and JSON-RPC is relayed between the HTTP
// requests of that session and the process unchanged (ADR-0003 §3). The proxy issues the session
// ID; binding it to the user is the job of the layer in front (mcpsession).
type Bridge struct {
	c BridgeConfig

	mu       sync.Mutex
	sessions map[string]*session  // legacy era, by session ID
	users    map[string]*userConn // modern era, by user
	cap      capability           // whether the upstream speaks the modern era
	next     atomic.Uint64        // proxy ids and tokens, unique across every userConn

	probeTimeout time.Duration // bounds the server/discover probe; tests shorten it
}

// NewBridge returns the bridge for c.Manager's upstream.
func NewBridge(c BridgeConfig) *Bridge {
	if c.Log == nil {
		c.Log = slog.New(slog.DiscardHandler)
	}
	return &Bridge{c: c, sessions: map[string]*session{}, users: map[string]*userConn{},
		cap: capability{clock: c.Manager.cfg.Clock}, probeTimeout: defaultProbeTimeout}
}

// newSessionID is 32 random bytes, base64url: visible ASCII, as the transport requires.
func newSessionID() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func (b *Bridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id, ok := b.c.Identity(r)
	if !ok {
		http.Error(w, "not authenticated", http.StatusUnauthorized)
		return
	}
	ids := r.Header.Values(sessionHeader)
	if len(ids) > 1 {
		http.Error(w, "send one "+sessionHeader, http.StatusBadRequest)
		return
	}
	key := ""
	if len(ids) == 1 {
		key = ids[0]
	}
	switch r.Method {
	case http.MethodPost:
		b.post(w, r, id, key)
	case http.MethodGet:
		b.get(w, r, id, key)
	case http.MethodDelete:
		b.delete(w, r, id, key)
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (b *Bridge) post(w http.ResponseWriter, r *http.Request, id Identity, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		if mbe := (*http.MaxBytesError)(nil); errors.As(err, &mbe) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "cannot read the request body", http.StatusBadRequest)
		return
	}
	msg, env, rpcErr := parseMessage(body)
	if rpcErr != nil {
		writeRPCError(w, http.StatusBadRequest, nil, rpcErr.code, rpcErr.msg)
		return
	}
	var s *session
	var release func()
	if key == "" && env.Method != "initialize" {
		if meta, err := readMeta(msg); err == nil && meta.modern() {
			b.modernPost(w, r, id, msg, env, meta)
			return
		} else if err != nil && looksModern(msg) {
			writeRPCError(w, http.StatusBadRequest, nil, codeInvalidRequest, "Invalid Request: "+err.Error())
			return
		}
	}
	if key == "" {
		if env.Method != "initialize" || env.kind() != kindRequest {
			legacyFallback(w)
			return
		}
		if s, release = b.start(w, r, id); s == nil {
			return
		}
	} else if s, release = b.session(w, r, id, key); s == nil {
		return
	}
	defer release()
	switch env.kind() {
	case kindRequest:
		s.request(w, r, msg, env.ID, key == "")
	case kindNotification, kindResponse:
		if err := s.p.Send(r.Context(), msg); err != nil {
			s.sendFailed(w, r, err)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

func (b *Bridge) get(w http.ResponseWriter, r *http.Request, id Identity, key string) {
	if key == "" {
		// Only a legacy MCP session has a GET stream or a DELETE; the modern era has neither
		// (2026-07-28 transports).
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed without "+sessionHeader, http.StatusMethodNotAllowed)
		return
	}
	s, release := b.session(w, r, id, key)
	if s == nil {
		return
	}
	defer release()
	s.listen(w, r)
}

func (b *Bridge) delete(w http.ResponseWriter, r *http.Request, id Identity, key string) {
	if key == "" {
		// Only a legacy MCP session has a GET stream or a DELETE; the modern era has neither
		// (2026-07-28 transports).
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed without "+sessionHeader, http.StatusMethodNotAllowed)
		return
	}
	s, release := b.session(w, r, id, key)
	if s == nil {
		return
	}
	release()
	b.c.Manager.Stop(s.key)
	s.end() // open streams finish now, not when the process is gone
	w.WriteHeader(http.StatusNoContent)
}

// startFailed answers a request whose process could not be had: 503 with Retry-After when every
// process under the caps is in use, else 502.
func (b *Bridge) startFailed(w http.ResponseWriter, r *http.Request, id Identity, err error) {
	if errors.Is(err, errBusy) {
		b.c.Log.WarnContext(r.Context(), "stdio process cap full: every process is in use", "request_id", logging.RequestID(r.Context()),
			"reason", "process_cap", "user_key", id.Subject)
		w.Header().Set("Retry-After", busyRetryAfter)
		http.Error(w, "upstream busy", http.StatusServiceUnavailable)
		return
	}
	b.c.Log.WarnContext(r.Context(), "cannot start the upstream process", "request_id", logging.RequestID(r.Context()), "error", err)
	http.Error(w, "upstream unavailable", http.StatusBadGateway)
}

// start spawns the process of a new session. On failure it has answered the request and returns
// nil.
func (b *Bridge) start(w http.ResponseWriter, r *http.Request, id Identity) (*session, func()) {
	key, err := newSessionID()
	if err != nil {
		b.c.Log.ErrorContext(r.Context(), "cannot create a session ID", "request_id", logging.RequestID(r.Context()), "error", err)
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return nil, nil
	}
	p, release, err := b.c.Manager.Get(r.Context(), key, id)
	if err != nil {
		b.startFailed(w, r, id, err)
		return nil, nil
	}
	s := &session{key: key, p: p, log: b.c.Log.With("process", p.PID(), "user_key", id.Subject), byID: map[string]*stream{}}
	b.mu.Lock()
	b.sessions[key] = s
	b.mu.Unlock()
	go func() {
		s.pump()
		b.mu.Lock()
		delete(b.sessions, key)
		b.mu.Unlock()
	}()
	return s, release
}

// session finds the live session for key and leases its process. A session that does not exist,
// has ended or serves another user gets 404, after which the client starts a new one (2025-11-25
// transports, Session Management rules 3 and 4). A request whose MCP-Protocol-Version names
// another version than the session negotiated gets 400 (Protocol Version Header). Either way it
// returns nil, with nothing leased.
func (b *Bridge) session(w http.ResponseWriter, r *http.Request, id Identity, key string) (*session, func()) {
	b.mu.Lock()
	s := b.sessions[key]
	b.mu.Unlock()
	if s == nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return nil, nil
	}
	_, release, err := b.c.Manager.leaseKey(r.Context(), key, id)
	if err != nil {
		if errors.Is(err, ErrUserMismatch) {
			// The binding in front makes this unreachable; it is checked again where the process is.
			b.c.Log.WarnContext(r.Context(), "session of another user", "request_id", logging.RequestID(r.Context()),
				"reason", "session_user_mismatch", "user_key", id.Subject)
		}
		http.Error(w, "session not found", http.StatusNotFound)
		return nil, nil
	}
	if v := r.Header.Get(protocolVersionHeader); v != "" && !s.speaks(v) {
		// The upstream never sees headers, so the transport's rule is ours (2025-11-25 transports,
		// Protocol Version Header): an unsupported version gets 400. No header passes.
		release()
		writeRPCError(w, http.StatusBadRequest, nil, codeInvalidRequest,
			"Bad Request: unsupported "+protocolVersionHeader+"; this MCP session negotiated "+s.negotiated())
		return nil, nil
	}
	return s, release
}

// protocolVersionHeader carries the negotiated version on every request after initialize.
const protocolVersionHeader = "MCP-Protocol-Version"

// speaks reports whether v is the version this MCP session negotiated. Before one is known (an
// InitializeResult without it), any version passes: the upstream decides.
func (s *session) speaks(v string) bool {
	n := s.negotiated()
	return n == "" || v == n
}

func (s *session) negotiated() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version
}

// recordVersion keeps the protocol version from an InitializeResult.
func (s *session) recordVersion(result json.RawMessage) {
	var r struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if json.Unmarshal(result, &r) == nil {
		s.mu.Lock()
		s.version = r.ProtocolVersion
		s.mu.Unlock()
	}
}

// session is one MCP session: its process and the HTTP streams its messages go out on.
type session struct {
	key string
	p   *Process
	log *slog.Logger

	mu        sync.Mutex
	posts     []*stream          // in-flight requests' streams, oldest first
	byID      map[string]*stream // the same, by JSON-RPC request id
	getStream *stream            // the GET stream, if one is open
	queue     [][]byte           // notifications waiting for a stream
	ended     bool
	version   string // the protocol version the upstream's InitializeResult negotiated; "" until then
}

// stream is one SSE response: a request's (POST) or the session's GET stream.
type stream struct {
	id    json.RawMessage // the request's id; nil for the GET stream
	out   chan []byte     // messages to write
	final chan []byte     // the request's response; nil for the GET stream
	done  chan struct{}   // closed when the stream must end: the session ended or a newer GET replaced it
	once  sync.Once
}

func newStream(id json.RawMessage) *stream {
	st := &stream{id: id, out: make(chan []byte, streamBuffer), done: make(chan struct{})}
	if id != nil {
		st.final = make(chan []byte, 1)
	}
	return st
}

func (st *stream) end() { st.once.Do(func() { close(st.done) }) }

// request relays one client request and streams what the upstream sends for it, ending with its
// response. For the initialize that started the session, nothing is written until the upstream
// sends something, so an initialize it fails or never answers gets no session ID (2025-11-25
// transports, Session Management: the ID goes on the response carrying the InitializeResult), and
// never a 404 the client would read as a legacy HTTP+SSE server (backwards compatibility). If the
// upstream sent other messages first, the stream has already started with the ID; it is not held
// back, since an upstream may wait for a ping to be answered. Either way, an initialize that ends
// without a result stops the process, so the ID leads nowhere: the client gets 404 and starts again.
func (s *session) request(w http.ResponseWriter, r *http.Request, msg []byte, id json.RawMessage, initialize bool) {
	st := newStream(id)
	if !s.addPost(st) {
		if s.isEnded() {
			http.Error(w, "session not found", http.StatusNotFound)
			return
		}
		writeRPCError(w, http.StatusBadRequest, id, codeInvalidRequest, "a request with this id is already in flight")
		return
	}
	// For initialize, every way out but a written result stops the process, unless the process is
	// already ending (stop says so), which keeps a crash logged as a crash.
	succeeded, stop, reason := false, true, "client_disconnected"
	if initialize {
		defer func() {
			if !succeeded {
				if stop {
					s.p.stop("initialize_failed")
				}
				s.log.WarnContext(r.Context(), "initialize failed: no MCP session", "request_id", logging.RequestID(r.Context()), "reason", reason)
			}
		}()
	}
	if err := s.p.Send(r.Context(), msg); err != nil {
		s.dropPost(st)
		if initialize {
			reason = "send_failed"
			http.Error(w, "upstream unavailable", http.StatusBadGateway)
			return
		}
		s.sendFailed(w, r, err)
		return
	}
	var out *sse // started on the first message, so initialize can still go without a session ID
	begin := func() {
		if out == nil {
			if initialize {
				w.Header().Set(sessionHeader, s.key)
			}
			started := startSSE(w)
			out = &started
		}
	}
	if !initialize {
		begin()
	}
	// send writes m; a message that cannot be written is handled as undeliverable.
	send := func(m []byte) bool {
		begin()
		if out.write(m) != nil {
			s.undeliverable(m, "client_disconnected")
			return false
		}
		return true
	}
	for {
		select {
		case m := <-st.out:
			if !send(m) {
				s.dropPost(st)
				return
			}
		case m := <-st.final:
			answered := true
			if initialize { // only here: parsing a large result just to relay it costs for nothing
				if env, err := parseEnvelope(m); err != nil || env.Error != nil {
					answered, reason = false, "initialize_error"
					if out == nil {
						w.Header().Set("Content-Type", "application/json")
						_, _ = w.Write(m)
						return
					}
				} else {
					s.recordVersion(env.Result) // before the result reaches the client
				}
			}
			ok := true
			for drained := false; !drained; { // what the upstream sent before the response comes first
				select {
				case o := <-st.out:
					if ok {
						ok = send(o)
					} else {
						s.undeliverable(o, "client_disconnected")
					}
				default:
					drained = true
				}
			}
			succeeded = ok && send(m) && answered
			return
		case <-st.done:
			// The session ended: stdout closed (Process stops a process that does not exit by
			// itself) or DELETE stopped it.
			stop, reason = false, "session_ended"
			if initialize && out == nil {
				http.Error(w, "upstream unavailable", http.StatusBadGateway)
				return
			}
			begin()
			_ = out.write(rpcError(id, codeInternalError, "upstream process exited"))
			return
		case <-r.Context().Done():
			// A disconnect is not a cancellation (2025-11-25 transports); the request stays with the
			// upstream, and its response, when it comes, is dropped.
			s.dropPost(st)
			return
		}
	}
}

// listen serves the session's GET stream until the client leaves, the session ends or a newer GET
// replaces it. Each message goes out on one stream only (2025-11-25 transports, Multiple
// Connections), so the newest GET stream is the one used.
func (s *session) listen(w http.ResponseWriter, r *http.Request) {
	st := newStream(nil)
	if !s.setListen(st) {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	defer s.clearListen(st)
	sse := startSSE(w)
	q := newQuiet(s.p.clock)
	defer q.stop()
	for {
		select {
		case m := <-st.out:
			q.writing()
			if sse.write(m) != nil {
				s.undeliverable(m, "client_disconnected")
				return
			}
			s.refill(st)
		case <-q.elapsed():
			q.writing()
			if sse.keepAlive() != nil {
				return
			}
		case <-st.done:
			return
		case <-r.Context().Done():
			return
		}
	}
}

func (s *session) sendFailed(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, ErrExited) {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	s.log.WarnContext(r.Context(), "relaying to the upstream process failed", "request_id", logging.RequestID(r.Context()), "error", err)
	http.Error(w, "upstream unavailable", http.StatusBadGateway)
}

func (s *session) isEnded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ended
}

func (s *session) addPost(st *stream) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := string(st.id)
	if s.ended || s.byID[k] != nil {
		return false
	}
	s.byID[k] = st
	s.posts = append(s.posts, st)
	return true
}

// dropPost removes a request's stream; upstream requests still waiting on it are answered.
func (s *session) dropPost(st *stream) {
	s.mu.Lock()
	s.removePostLocked(st)
	s.mu.Unlock()
	for {
		select {
		case m := <-st.out:
			s.undeliverable(m, "client_disconnected")
		default:
			return
		}
	}
}

func (s *session) removePostLocked(st *stream) {
	if s.byID[string(st.id)] == st {
		delete(s.byID, string(st.id))
	}
	for i, p := range s.posts {
		if p == st {
			s.posts = append(s.posts[:i], s.posts[i+1:]...)
			break
		}
	}
}

func (s *session) setListen(st *stream) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return false
	}
	if s.getStream != nil {
		s.getStream.end()
	}
	s.getStream = st
	s.refillLocked(st)
	return true
}

// refill moves queued notifications onto the GET stream, as many as it has room for.
func (s *session) refill(st *stream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getStream == st {
		s.refillLocked(st)
	}
}

func (s *session) refillLocked(st *stream) {
	for len(s.queue) > 0 && len(st.out) < cap(st.out) {
		st.out <- s.queue[0]
		s.queue = s.queue[1:]
	}
}

func (s *session) clearListen(st *stream) {
	s.mu.Lock()
	if s.getStream == st {
		s.getStream = nil
	}
	s.mu.Unlock()
	for {
		select {
		case m := <-st.out:
			s.undeliverable(m, "client_disconnected")
		default:
			return
		}
	}
}

// pump routes every upstream message until stdout closes, then ends the session.
func (s *session) pump() {
	for msg := range s.p.Messages() {
		s.route(msg)
	}
	s.end()
}

// end ends the MCP session: every open stream finishes and no new one is accepted.
func (s *session) end() {
	s.mu.Lock()
	s.ended = true
	streams := append([]*stream{}, s.posts...)
	if s.getStream != nil {
		streams = append(streams, s.getStream)
	}
	s.queue = nil
	s.mu.Unlock()
	for _, st := range streams {
		st.end()
	}
}

// route sends one upstream message to its stream: a response to the request waiting for it; a
// request or notification to the oldest in-flight request, else the GET stream.
func (s *session) route(msg []byte) {
	env, err := parseEnvelope(msg)
	if err != nil || env.kind() == kindInvalid {
		s.log.Warn("upstream wrote a line that is not a JSON-RPC message", "reason", "invalid_upstream_message")
		return
	}
	s.mu.Lock()
	if env.kind() == kindResponse {
		st := s.byID[string(compactID(env.ID))]
		if st != nil {
			s.removePostLocked(st)
		}
		s.mu.Unlock()
		if st == nil {
			s.log.Warn("upstream response for no waiting request", "reason", "response_unrouted")
			return
		}
		st.final <- msg
		return
	}
	var st *stream
	switch {
	case len(s.posts) > 0:
		st = s.posts[0]
	case s.getStream != nil && (len(s.queue) == 0 || env.kind() == kindRequest): // queued notifications go first
		st = s.getStream
	}
	if st != nil {
		select {
		case st.out <- msg:
			s.mu.Unlock()
			return
		default:
		}
		s.mu.Unlock()
		s.undeliverable(msg, "stream_full")
		return
	}
	if env.kind() == kindNotification {
		if s.getStream != nil {
			defer s.refill(s.getStream)
		}
		if len(s.queue) == maxQueued {
			s.queue = s.queue[1:]
			s.log.Warn("upstream notification dropped", "reason", "queue_full")
		}
		s.queue = append(s.queue, msg)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.undeliverable(msg, "client_not_connected")
}

// undeliverable handles an upstream message no stream could take. A notification is dropped; a
// request is answered at once with an error, so the upstream never waits on it.
func (s *session) undeliverable(msg []byte, reason string) {
	env, err := parseEnvelope(msg)
	if err != nil {
		return
	}
	if env.kind() != kindRequest {
		s.log.Warn("upstream notification dropped", "reason", reason, "method", env.Method)
		return
	}
	s.log.Warn("upstream request answered with an error: no client stream", "reason", reason, "method", env.Method)
	s.p.sendDetached(rpcError(env.ID, codeInternalError, "client not connected"))
}

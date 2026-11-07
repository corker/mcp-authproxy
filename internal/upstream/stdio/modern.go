//go:build linux || darwin

package stdio

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/byward-dev/byward/internal/logging"
)

// The modern era (2026-07-28): no sessions, every request carries its own metadata, and one
// upstream process serves all of a user's MCP clients (ADR-0003 §4). Their requests share one
// stdio channel, so the proxy rewrites each request id and progress token to one of its own and
// maps them back on the way out; nothing else in a message is touched.
const (
	// modernVersion is the first protocol version without initialize; dates compare as strings.
	modernVersion = "2026-07-28"
	// defaultProbeTimeout bounds the server/discover probe; a legacy server may never answer it.
	defaultProbeTimeout = 10 * time.Second
	// legacyVerdictTTL is how long "this upstream is not modern" is kept, so one failed probe does
	// not hold for the life of the proxy.
	legacyVerdictTTL = 10 * time.Minute
	// probeID is the id of the proxy's own server/discover.
	probeID = `"byw-probe"`
)

var (
	pathID       = jsonPath{"id"}
	pathVersion  = jsonPath{"params", "_meta", "io.modelcontextprotocol/protocolVersion"}
	pathToken    = jsonPath{"params", "_meta", "progressToken"}
	pathLogLevel = jsonPath{"params", "_meta", "io.modelcontextprotocol/logLevel"}
	pathProgress = jsonPath{"params", "progressToken"}
	pathName     = jsonPath{"params", "name"}
	pathURI      = jsonPath{"params", "uri"}
	pathSub      = jsonPath{"params", "_meta", "io.modelcontextprotocol/subscriptionId"}
	pathResSub   = jsonPath{"result", "_meta", "io.modelcontextprotocol/subscriptionId"}
	pathCancelID = jsonPath{"params", "requestId"}
)

// Request metadata headers (2026-07-28 streamable-http). A stdio upstream never sees headers, so
// the bridge is the server that validates them (ADR-0004 §3, amended).
const (
	headerProtocolVersion = "MCP-Protocol-Version"
	headerMethod          = "Mcp-Method"
	headerName            = "Mcp-Name"
	codeHeaderMismatch    = -32020
)

// nameSource is the body field Mcp-Name mirrors, for the methods that require it.
var nameSource = map[string]jsonPath{"tools/call": pathName, "prompts/get": pathName, "resources/read": pathURI}

// probeRequest is the proxy's server/discover: does the upstream speak the modern era?
var probeRequest = []byte(`{"jsonrpc":"2.0","id":` + probeID + `,"method":"server/discover","params":{"_meta":{` +
	`"io.modelcontextprotocol/protocolVersion":"` + modernVersion + `",` +
	`"io.modelcontextprotocol/clientInfo":{"name":"byward","version":"probe"},` +
	`"io.modelcontextprotocol/clientCapabilities":{}}}}`)

// requestMeta is what the proxy reads of a client message, found in one pass.
type requestMeta struct {
	id, token *span  // nil when absent
	logs      bool   // _meta asks for notifications/message
	version   string // _meta protocol version; "" when absent or not a string
}

// readMeta locates a message's id, progress token, log level and protocol version. Ambiguous JSON
// is refused: the ids are rewritten, and a wrong guess would route one client's answer to another.
func readMeta(msg []byte) (requestMeta, error) {
	at, err := locate(msg, pathID, pathToken, pathLogLevel, pathVersion) // the method is checked by parseMessage, in both eras
	if err != nil {
		return requestMeta{}, err
	}
	m := requestMeta{id: at[0], token: at[1], logs: at[2] != nil}
	if at[3] != nil {
		_ = json.Unmarshal(at[3].value(msg), &m.version)
	}
	return m, nil
}

// looksModern reports whether a message readMeta refused names a modern protocol version anyway,
// so its refusal can say why instead of sending the client to initialize.
func looksModern(msg []byte) bool {
	var m struct {
		Params struct {
			Meta struct {
				Version string `json:"io.modelcontextprotocol/protocolVersion"`
			} `json:"_meta"`
		} `json:"params"`
	}
	return json.Unmarshal(msg, &m) == nil && m.Params.Meta.Version >= modernVersion
}

// modern reports whether the message asks for the modern era: a protocol version from 2026-07-28.
func (m requestMeta) modern() bool { return m.version >= modernVersion }

// checkRequest refuses a request whose id or progress token is not a string or an integer.
func (m requestMeta) checkRequest(msg []byte) error {
	if m.id == nil {
		return errAmbiguous
	}
	if err := scalarID(m.id.value(msg)); err != nil {
		return err
	}
	if m.token != nil {
		return scalarID(m.token.value(msg))
	}
	return nil
}

// checkHeaders applies the transport's header rules to a request: MCP-Protocol-Version and
// Mcp-Method, and Mcp-Name where the method mirrors one, must be present and equal the body's
// values (case-sensitive, after decoding a Base64 sentinel). Notification POSTs have no header
// requirements in this revision.
func checkHeaders(h http.Header, method, version string, msg []byte) error {
	type mirrored struct {
		name, value string
		base64      bool // may arrive in the Base64 sentinel form
	}
	want := []mirrored{{headerProtocolVersion, version, false}, {headerMethod, method, false}}
	if path, ok := nameSource[method]; ok {
		at, err := locate(msg, path)
		if err != nil {
			return err
		}
		if at[0] != nil { // absent: the body is the upstream's to refuse
			var v string
			if json.Unmarshal(at[0].value(msg), &v) != nil {
				return fmt.Errorf("%s mirrors a value that is not a string", headerName)
			}
			want = append(want, mirrored{headerName, v, true})
		}
	}
	for _, w := range want {
		vals := h.Values(w.name)
		if len(vals) != 1 {
			return fmt.Errorf("header %s missing or repeated", w.name)
		}
		got := vals[0]
		if !plainHeaderValue(got) {
			return fmt.Errorf("header %s has characters a header value may not", w.name)
		}
		if w.base64 { // Mcp-Name, and Mcp-Param-* should they come, only
			var err error
			if got, err = headerValue(got); err != nil {
				return fmt.Errorf("header %s: %w", w.name, err)
			}
		}
		if got != w.value {
			return fmt.Errorf("header %s does not match the body", w.name)
		}
	}
	return nil
}

// plainHeaderValue reports whether v holds only what a header value may: visible ASCII, space and
// tab (2026-07-28 streamable-http, Value Encoding). Anything else must come Base64-encoded.
func plainHeaderValue(v string) bool {
	for i := 0; i < len(v); i++ {
		if c := v[i]; (c < 0x20 || c > 0x7e) && c != '\t' {
			return false
		}
	}
	return true
}

// headerValue decodes the Base64 sentinel form =?base64?…?= that clients use for values that are
// not plain ASCII; any other value is taken as it is.
func headerValue(v string) (string, error) {
	inner, ok := strings.CutPrefix(v, "=?base64?")
	if !ok {
		return v, nil
	}
	inner, ok = strings.CutSuffix(inner, "?=")
	if !ok {
		return "", errors.New("malformed Base64 sentinel")
	}
	b, err := base64.StdEncoding.DecodeString(inner)
	if err != nil {
		return "", errors.New("malformed Base64 sentinel")
	}
	return string(b), nil
}

// capability is what the proxy has learned about the upstream's era.
type capability struct {
	clock       Clock
	mu          sync.Mutex
	modern      bool
	legacyUntil time.Time
}

func (c *capability) isModern() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.modern
}

// legacy reports a fresh "not modern" verdict.
func (c *capability) legacy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.modern && c.clock.Now().Before(c.legacyUntil)
}

func (c *capability) set(modern bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.modern = modern
	if !modern {
		c.legacyUntil = c.clock.Now().Add(legacyVerdictTTL)
	}
}

// userConn is a user's modern-era upstream process and the requests in flight on it.
type userConn struct {
	p   *Process
	log *slog.Logger

	ids *atomic.Uint64 // the Bridge's: proxy ids never repeat across userConns

	mu      sync.Mutex
	byID    map[string]*modernRequest // by proxy id
	byToken map[string]*modernRequest // by proxy progress token
	probe   chan []byte               // the probe's answer
	ended   chan struct{}

	probeOnce sync.Once // concurrent first requests share one probe
	modern    bool
}

// modernRequest is one client request in flight on a userConn.
type modernRequest struct {
	id, token []byte // the client's own, restored on the way out; token nil when none
	proxyID   string
	proxyTok  string
	logs      bool // asked for notifications/message (io.modelcontextprotocol/logLevel)
	out       chan []byte
	final     chan []byte
	listen    bool          // a subscriptions/listen: its graceful end carries the id in result._meta too
	torn      chan struct{} // closed when the upstream tears down this subscriptions/listen
	tornOnce  sync.Once
}

// modernPost serves one session-less modern-era POST; meta is the message's, already read.
func (b *Bridge) modernPost(w http.ResponseWriter, r *http.Request, id Identity, msg []byte, env envelope, meta requestMeta) {
	if b.cap.legacy() {
		legacyFallback(w)
		return
	}
	switch env.kind() {
	case kindResponse: // the modern era has no server-to-client requests to answer (MRTR instead)
		writeRPCError(w, http.StatusBadRequest, nil, codeInvalidRequest, "Invalid Request: no request awaits a client response")
		return
	case kindNotification:
		if env.Method == "notifications/cancelled" {
			// Over HTTP, closing the response stream cancels; an id here could be any client's.
			w.WriteHeader(http.StatusAccepted)
			return
		}
	case kindRequest:
		if err := meta.checkRequest(msg); err != nil { // refused before any process starts
			writeRPCError(w, http.StatusBadRequest, nil, codeInvalidRequest, "Invalid Request: "+err.Error())
			return
		}
		if err := checkHeaders(r.Header, env.Method, meta.version, msg); err != nil {
			writeRPCError(w, http.StatusBadRequest, meta.id.value(msg), codeHeaderMismatch, "Header mismatch: "+err.Error())
			return
		}
	}
	key := "user:" + id.user()
	p, release, err := b.c.Manager.Get(r.Context(), key, id)
	if err != nil {
		b.startFailed(w, r, id, err)
		return
	}
	defer release()
	c := b.connFor(key, p, id)
	if !b.speaksModern(r.Context(), c) {
		legacyFallback(w)
		return
	}
	if env.kind() == kindNotification {
		if err := p.Send(r.Context(), msg); err != nil {
			http.Error(w, "upstream unavailable", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}
	c.request(w, r, msg, env.Method, meta)
}

// legacyFallback is the answer to a modern request the upstream cannot serve: no modern error
// code, so a dual-era client falls back to initialize (2026-07-28 transports, backwards
// compatibility).
func legacyFallback(w http.ResponseWriter) {
	writeRPCError(w, http.StatusBadRequest, nil, codeInvalidRequest,
		"Bad Request: this server speaks MCP 2025-11-25; start a session with initialize")
}

// connFor returns the userConn of the user's process p, creating it (and its pump) for a new
// process.
func (b *Bridge) connFor(key string, p *Process, id Identity) *userConn {
	b.mu.Lock()
	defer b.mu.Unlock()
	if c := b.users[key]; c != nil && c.p == p {
		return c
	}
	c := &userConn{p: p, ids: &b.next, log: b.c.Log.With("process", p.PID(), "user_key", id.Subject),
		byID: map[string]*modernRequest{}, byToken: map[string]*modernRequest{}, probe: make(chan []byte, 1), ended: make(chan struct{})}
	b.users[key] = c
	go func() {
		c.pump()
		b.mu.Lock()
		if b.users[key] == c { // a successor for a respawned process stays
			delete(b.users, key)
		}
		b.mu.Unlock()
		close(c.ended) // after the map is settled, so whoever sees it ended sees that too
	}()
	return c
}

// speaksModern decides the upstream's era: known modern, or probed on this process. A process
// that fails the probe is stopped; the verdict is kept for legacyVerdictTTL.
func (b *Bridge) speaksModern(ctx context.Context, c *userConn) bool {
	if b.cap.isModern() {
		return true
	}
	if !c.probed(ctx, b.probeTimeout) {
		b.cap.set(false)
		c.p.stop("not_modern")
		return false
	}
	b.cap.set(true)
	return true
}

// probed asks the process server/discover, once; a DiscoverResult means it speaks the modern era.
func (c *userConn) probed(ctx context.Context, timeout time.Duration) bool {
	c.probeOnce.Do(func() { c.modern = c.discover(ctx, timeout) })
	return c.modern
}

func (c *userConn) discover(ctx context.Context, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	if c.p.Send(ctx, probeRequest) != nil {
		return false
	}
	select {
	case m := <-c.probe:
		var res struct {
			Result struct {
				SupportedVersions []string `json:"supportedVersions"`
			} `json:"result"`
		}
		return json.Unmarshal(m, &res) == nil && len(res.Result.SupportedVersions) > 0
	case <-c.ended:
		return false
	case <-ctx.Done():
		return false
	}
}

// request relays one modern-era request under proxy-chosen ids.
func (c *userConn) request(w http.ResponseWriter, r *http.Request, msg []byte, method string, meta requestMeta) {
	var token []byte
	if meta.token != nil {
		token = meta.token.value(msg)
	}
	q := c.register(meta.id.value(msg), token, meta.logs, method == "subscriptions/listen")
	defer c.unregister(q)
	if err := c.p.Send(r.Context(), replace(msg, edit{meta.id, []byte(q.proxyID)}, edit{meta.token, []byte(q.proxyTok)})); err != nil {
		c.log.WarnContext(r.Context(), "relaying to the upstream process failed", "request_id", logging.RequestID(r.Context()), "error", err)
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	var silence *quiet // only a listen stream is long-lived enough to need a keep-alive
	if q.listen {
		silence = newQuiet(c.p.clock) // before the stream starts, as for the GET stream
	}
	defer silence.stop()
	out := startSSE(w)
	drain := func() { // what the upstream sent before the end goes out first
		for {
			select {
			case o := <-q.out:
				_ = out.write(o)
			default:
				return
			}
		}
	}
	finish := func(m []byte) { drain(); _ = out.write(m) }
	for {
		select {
		case m := <-q.out:
			silence.writing()
			if out.write(m) != nil {
				c.cancel(q)
				return
			}
			silence.writing()
		case <-silence.elapsed():
			silence.writing()
			if out.keepAlive() != nil { // the client is gone: as for a closed stream
				c.cancel(q)
				return
			}
			silence.writing()
		case m := <-q.final:
			finish(m)
			return
		case <-c.ended:
			finish(exitAnswer(q))
			return
		case <-q.torn:
			// The upstream ended the subscription. Over HTTP that is said by closing the stream; the
			// cancellation itself is stdio's (2026-07-28 cancellation).
			drain()
			return
		case <-r.Context().Done():
			c.cancel(q) // closing the stream cancels the request (2026-07-28 transports)
			return
		}
	}
}

// exitAnswer is what a request gets once its process has ended: its answer, if the upstream wrote
// it before exiting, else an error.
func exitAnswer(q *modernRequest) []byte {
	select {
	case m := <-q.final:
		return m
	default:
		return rpcError(q.id, codeInternalError, "upstream process exited")
	}
}

func (c *userConn) register(id, token []byte, logs, listen bool) *modernRequest {
	n := strconv.FormatUint(c.ids.Add(1), 10)
	c.mu.Lock()
	defer c.mu.Unlock()
	q := &modernRequest{id: id, proxyID: `"byw-` + n + `"`, logs: logs, listen: listen, out: make(chan []byte, streamBuffer),
		final: make(chan []byte, 1), torn: make(chan struct{})}
	c.byID[q.proxyID] = q
	if token != nil {
		q.token, q.proxyTok = token, `"byw-t`+n+`"`
		c.byToken[q.proxyTok] = q
	}
	return q
}

func (c *userConn) unregister(q *modernRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byID[q.proxyID] == q {
		delete(c.byID, q.proxyID)
	}
	if q.proxyTok != "" && c.byToken[q.proxyTok] == q {
		delete(c.byToken, q.proxyTok)
	}
}

// cancel tells the upstream to stop work on q, which nobody will read any more.
func (c *userConn) cancel(q *modernRequest) {
	c.unregister(q)
	c.p.sendDetached([]byte(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":` + q.proxyID + `}}`))
}

// pump routes every upstream message until stdout closes.
func (c *userConn) pump() {
	for msg := range c.p.Messages() {
		c.route(msg)
	}
}

// route sends one upstream message to the request it belongs to, under the client's own ids.
func (c *userConn) route(msg []byte) {
	env, err := parseEnvelope(msg)
	if err != nil || env.kind() == kindInvalid {
		c.log.Warn("upstream wrote a line that is not a JSON-RPC message", "reason", "invalid_upstream_message")
		return
	}
	switch env.kind() {
	case kindResponse:
		c.routeResponse(msg, string(compactID(env.ID)))
	case kindRequest: // a modern server MUST NOT write requests to stdout (2026-07-28 stdio)
		c.log.Warn("upstream sent a request, which the modern era does not allow", "reason", "upstream_request", "method", env.Method)
		c.p.sendDetached(rpcError(env.ID, codeInternalError, "server-to-client requests are not part of MCP 2026-07-28"))
	case kindNotification:
		c.routeNotification(msg, env.Method)
	}
}

func (c *userConn) routeResponse(msg []byte, proxyID string) {
	if proxyID == probeID {
		select {
		case c.probe <- msg:
		default:
		}
		return
	}
	q := c.byProxyID(proxyID)
	if q == nil {
		c.log.Warn("upstream response for no waiting request", "reason", "response_unrouted")
		return
	}
	paths := []jsonPath{pathID}
	if q.listen { // a graceful end names the subscription in result._meta too
		paths = append(paths, pathResSub)
	}
	var out []byte
	if at, err := locate(msg, paths...); err == nil && at[0] != nil {
		// This answers q by its id, so whatever a listen's _meta names is q's subscription: every
		// located id becomes the client's, never a proxy id.
		edits := make([]edit, len(at))
		for i, s := range at {
			edits[i] = edit{s, q.id}
		}
		out = replace(msg, edits...)
	} else {
		// Routable, but its id (or a listen's subscription id) cannot be placed with certainty: the
		// client gets an error, not silence, and never the proxy's id.
		c.log.Warn("upstream response is ambiguous", "reason", "response_ambiguous")
		out = rpcError(q.id, codeInternalError, "the upstream sent an ambiguous response")
	}
	select {
	case q.final <- out:
	default:
	}
}

// byProxyID is the request in flight under the proxy's id, or nil.
func (c *userConn) byProxyID(id string) *modernRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byID[id]
}

func (c *userConn) routeNotification(msg []byte, method string) {
	switch method {
	case "notifications/progress":
		at, err := locate(msg, pathProgress)
		if err != nil || at[0] == nil {
			c.drop(method, "progress_unrouted")
			return
		}
		c.mu.Lock()
		q := c.byToken[string(compactID(at[0].value(msg)))]
		c.mu.Unlock()
		if q == nil {
			c.drop(method, "progress_unrouted")
			return
		}
		c.deliver(q, replace(msg, edit{at[0], q.token}), method)
	case "notifications/message":
		// No correlation key on stdio (2026-07-28 logging): delivered only when exactly one request
		// in flight asked for log messages; otherwise it could reach the wrong client.
		c.mu.Lock()
		var only *modernRequest
		n := 0
		for _, q := range c.byID {
			if q.logs {
				only, n = q, n+1
			}
		}
		c.mu.Unlock()
		if n != 1 {
			c.drop(method, "log_unrouted")
			return
		}
		c.deliver(only, msg, method)
	case "notifications/cancelled": // the upstream tearing down a subscriptions/listen
		at, err := locate(msg, pathCancelID)
		var q *modernRequest
		if err == nil && at[0] != nil {
			q = c.byProxyID(string(compactID(at[0].value(msg))))
		}
		if q == nil || !q.listen { // only a subscription is the upstream's to end
			c.drop(method, "cancel_unrouted")
			return
		}
		q.tornOnce.Do(func() { close(q.torn) })
	default:
		c.routeSubscription(msg, method)
	}
}

// routeSubscription delivers a notification of a subscriptions/listen stream: its _meta names the
// listen request's id, the proxy's, which is put back to the client's.
func (c *userConn) routeSubscription(msg []byte, method string) {
	at, err := locate(msg, pathSub)
	if err != nil || at[0] == nil {
		c.drop(method, "notification_unrouted")
		return
	}
	q := c.byProxyID(string(compactID(at[0].value(msg))))
	if q == nil || !q.listen {
		c.drop(method, "subscription_unrouted")
		return
	}
	c.deliver(q, replace(msg, edit{at[0], q.id}), method)
}

func (c *userConn) deliver(q *modernRequest, msg []byte, method string) {
	select {
	case q.out <- msg:
	default:
		c.drop(method, "stream_full")
	}
}

func (c *userConn) drop(method, reason string) {
	c.log.Warn("upstream notification dropped", "reason", reason, "method", method)
}

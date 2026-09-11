package gateway

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// A small Redis client in the standard library: enough of the RESP wire
// protocol to run the shared counter's scripts, and nothing else. It exists so
// the binary keeps its zero-dependency shape; it is not a general client.
//
// Every operation has a short deadline (respTimeout). A store that does not
// answer in time is reported as an error to the caller, which then decides
// per process; the caller never waits longer than that on a request.

const respTimeout = 500 * time.Millisecond

// respError is an error reply from the server (-ERR …), as opposed to a
// connection failure. A script that is not loaded yet reports NOSCRIPT here.
type respError string

func (e respError) Error() string { return string(e) }

// errPoolBusy means every connection was in use for a whole deadline. It is a
// client-side condition, not a store outage, and callers treat it as such.
var errPoolBusy = errors.New("shared counter: every connection busy")

type respConn struct {
	c net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

type respClient struct {
	addr     string
	host     string
	user     string
	password string
	db       int
	useTLS   bool
	timeout  time.Duration
	dial     func() (net.Conn, error)

	slots chan struct{} // one token per connection the pool may hold open
	dials chan struct{} // how many connections may be opening at once
	mu    sync.Mutex
	idle  []*respConn
}

// newRESPClient parses redis://[user:password@]host[:port][/db] (or rediss://
// for TLS). The password, when given, travels in the URL — keep such a URL out
// of shell history and process listings on shared machines. No error message
// here ever repeats the URL.
func newRESPClient(raw string) (*respClient, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("shared counter: the URL could not be parsed; check the host and port")
	}
	c := &respClient{timeout: respTimeout, slots: make(chan struct{}, 64), dials: make(chan struct{}, 8)}
	switch u.Scheme {
	case "redis":
	case "rediss":
		c.useTLS = true
	default:
		return nil, fmt.Errorf("shared counter: the URL must start with redis:// or rediss://, got %s://", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		host = "127.0.0.1"
	}
	port := u.Port()
	if port == "" {
		port = "6379"
	}
	c.host = host
	c.addr = net.JoinHostPort(host, port)
	if u.User != nil {
		if pw, ok := u.User.Password(); ok {
			c.user, c.password = u.User.Username(), pw
		} else { // redis://password@host is a common shorthand
			c.password = u.User.Username()
		}
	}
	if p := strings.TrimPrefix(u.Path, "/"); p != "" {
		if c.db, err = strconv.Atoi(p); err != nil {
			return nil, errors.New("shared counter: the database number after the host must be a whole number")
		}
	}
	c.dial = func() (net.Conn, error) {
		d := &net.Dialer{Timeout: c.timeout}
		if c.useTLS {
			return tls.DialWithDialer(d, "tcp", c.addr, &tls.Config{ServerName: c.host, MinVersion: tls.VersionTLS12})
		}
		return d.Dial("tcp", c.addr)
	}
	return c, nil
}

// get returns a connection and whether it came from the pool. When every
// connection is in use it waits up to one deadline for a free one; only a wait
// that runs out is an error (errPoolBusy). A pooled connection may be stale
// (the store restarted); do skips every such connection and tries a fresh one.
func (p *respClient) get() (*respConn, bool, error) {
	if !p.acquire(p.slots) {
		return nil, false, errPoolBusy
	}
	p.mu.Lock()
	if n := len(p.idle); n > 0 {
		cn := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return cn, true, nil
	}
	p.mu.Unlock()
	// A cold pool under a burst must not storm the store with dozens of
	// handshakes at once; a few at a time is plenty on a fast network. The
	// wait for a turn is bounded like everything else here.
	if !p.acquire(p.dials) {
		p.drop()
		return nil, false, errPoolBusy
	}
	nc, err := p.dial()
	<-p.dials
	if err != nil {
		p.drop()
		return nil, false, err
	}
	cn := &respConn{c: nc, r: bufio.NewReader(nc), w: bufio.NewWriter(nc)}
	if p.password != "" {
		args := []string{"AUTH", p.password}
		if p.user != "" {
			args = []string{"AUTH", p.user, p.password}
		}
		if _, err := p.doOn(cn, args...); err != nil {
			p.discard(cn)
			return nil, false, err
		}
	}
	if p.db != 0 {
		if _, err := p.doOn(cn, "SELECT", strconv.Itoa(p.db)); err != nil {
			p.discard(cn)
			return nil, false, err
		}
	}
	return cn, false, nil
}

// acquire takes a token from a bounded channel, waiting at most one deadline.
func (p *respClient) acquire(ch chan struct{}) bool {
	select {
	case ch <- struct{}{}:
		return true
	default:
	}
	t := time.NewTimer(p.timeout)
	defer t.Stop()
	select {
	case ch <- struct{}{}:
		return true
	case <-t.C:
		return false
	}
}

func (p *respClient) put(cn *respConn) {
	p.mu.Lock()
	p.idle = append(p.idle, cn)
	p.mu.Unlock()
	<-p.slots
}

func (p *respClient) discard(cn *respConn) {
	_ = cn.c.Close()
	p.drop()
}

func (p *respClient) drop() { <-p.slots }

// staleRead says whether an error means a pooled connection had been closed by
// the other side before anything was executed: an immediate end-of-stream or
// reset on the first read. Only that is safe to retry; a timeout is not, the
// command may well have run.
func staleRead(err error) bool {
	if err == nil {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return false
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
}

// do sends one command and returns its reply: string (simple or bulk), int64,
// []any (array), nil (null), or an error. A respError is the server saying no;
// any other error means the connection is unusable and has been dropped. An
// idle connection the store has closed (it restarted) is discarded and the
// next one tried, for as many stale idle connections as the pool holds;
// nothing ran on any of them. A failure on a fresh connection is final.
func (p *respClient) do(args ...string) (any, error) {
	for {
		cn, reused, err := p.get()
		if err != nil {
			return nil, err
		}
		v, err := p.doOn(cn, args...)
		var re respError
		if err == nil || errors.As(err, &re) {
			p.put(cn)
			return v, err
		}
		p.discard(cn)
		if reused && staleRead(err) {
			continue
		}
		return nil, err
	}
}

func (p *respClient) doOn(cn *respConn, args ...string) (any, error) {
	if err := cn.c.SetDeadline(time.Now().Add(p.timeout)); err != nil {
		return nil, err
	}
	if err := writeCommand(cn.w, args); err != nil {
		return nil, err
	}
	return readReply(cn.r)
}

// eval runs a script by its SHA, loading it on the first NOSCRIPT.
func (p *respClient) eval(script, sha string, keys []string, args ...string) (any, error) {
	call := func(cmd, body string) (any, error) {
		a := make([]string, 0, 3+len(keys)+len(args))
		a = append(a, cmd, body, strconv.Itoa(len(keys)))
		a = append(a, keys...)
		a = append(a, args...)
		return p.do(a...)
	}
	v, err := call("EVALSHA", sha)
	var re respError
	if errors.As(err, &re) && strings.HasPrefix(string(re), "NOSCRIPT") {
		return call("EVAL", script)
	}
	return v, err
}

func writeCommand(w *bufio.Writer, args []string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, a := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(a), a); err != nil {
			return err
		}
	}
	return w.Flush()
}

func readLine(r *bufio.Reader) (string, error) {
	s, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(s) < 2 || s[len(s)-2] != '\r' {
		return "", errors.New("shared counter: malformed reply")
	}
	return s[:len(s)-2], nil
}

func readReply(r *bufio.Reader) (any, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if line == "" {
		return nil, errors.New("shared counter: empty reply")
	}
	body := line[1:]
	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return nil, respError(body)
	case ':':
		n, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("shared counter: bad integer reply %q", body)
		}
		return n, nil
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, fmt.Errorf("shared counter: bad bulk length %q", body)
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, fmt.Errorf("shared counter: bad array length %q", body)
		}
		if n < 0 {
			return nil, nil
		}
		out := make([]any, 0, n)
		for i := 0; i < n; i++ {
			v, err := readReply(r)
			var re respError
			if err != nil && !errors.As(err, &re) {
				return nil, err
			}
			if err != nil {
				out = append(out, re)
			} else {
				out = append(out, v)
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("shared counter: unknown reply type %q", line[0])
}

// Typed readers for script replies; a wrong shape is a bug in the script, and
// it is reported rather than guessed around.
func asInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	}
	return 0, false
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

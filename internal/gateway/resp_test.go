package gateway

import (
	"bufio"
	"bytes"
	"net"
	"strings"
	"testing"
)

func TestRESPReadsEveryReplyType(t *testing.T) {
	in := "+OK\r\n-ERR nope\r\n:42\r\n$5\r\nhello\r\n$-1\r\n*3\r\n:1\r\n$2\r\nhi\r\n-NOSCRIPT x\r\n*-1\r\n"
	r := bufio.NewReader(strings.NewReader(in))
	if v, err := readReply(r); err != nil || v != "OK" {
		t.Fatalf("simple string: %v %v", v, err)
	}
	if _, err := readReply(r); err == nil || err.Error() != "ERR nope" {
		t.Fatalf("error reply: %v", err)
	}
	if v, err := readReply(r); err != nil || v != int64(42) {
		t.Fatalf("integer: %v %v", v, err)
	}
	if v, err := readReply(r); err != nil || v != "hello" {
		t.Fatalf("bulk: %v %v", v, err)
	}
	if v, err := readReply(r); err != nil || v != nil {
		t.Fatalf("null bulk: %v %v", v, err)
	}
	v, err := readReply(r)
	arr, ok := v.([]any)
	if err != nil || !ok || len(arr) != 3 || arr[0] != int64(1) || arr[1] != "hi" {
		t.Fatalf("array: %v %v", v, err)
	}
	if _, isErr := arr[2].(respError); !isErr {
		t.Fatalf("an error inside an array is kept as a value: %T", arr[2])
	}
	if v, err := readReply(r); err != nil || v != nil {
		t.Fatalf("null array: %v %v", v, err)
	}
}

func TestRESPWritesCommandsAsBulkArrays(t *testing.T) {
	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	if err := writeCommand(w, []string{"HSET", "k", "f", "v w"}); err != nil {
		t.Fatal(err)
	}
	if got := b.String(); got != "*4\r\n$4\r\nHSET\r\n$1\r\nk\r\n$1\r\nf\r\n$3\r\nv w\r\n" {
		t.Fatalf("wire = %q", got)
	}
}

func TestRESPClientParsesURLsAndFallsBackOnNoScript(t *testing.T) {
	for raw, want := range map[string]string{
		"redis://localhost":                 "127.0.0.1:6379|||",
		"redis://10.0.0.5:6380/2":           "10.0.0.5:6380||2|",
		"rediss://:s3cret@cache.example":    "cache.example:6379|s3cret||tls",
		"redis://s3cret@cache.example:7000": "cache.example:7000|s3cret||",
	} {
		c, err := newRESPClient(raw)
		if err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		tlsMark := ""
		if c.useTLS {
			tlsMark = "tls"
		}
		db := ""
		if c.db != 0 {
			db = "2"
		}
		addr := c.addr
		if c.host == "localhost" {
			addr = "127.0.0.1:6379"
		}
		if got := addr + "|" + c.password + "|" + db + "|" + tlsMark; got != want {
			t.Errorf("%s: got %q want %q", raw, got, want)
		}
	}
	if _, err := newRESPClient("http://x"); err == nil {
		t.Fatal("a non-redis scheme must be refused")
	}

	// A fake server: the first EVALSHA gets NOSCRIPT, the EVAL that follows
	// is answered. The client must retry with the script body by itself.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		for i := 0; ; i++ {
			// read one command: array header then bulk strings
			line, err := readLine(r)
			if err != nil {
				return
			}
			n := 0
			for _, ch := range line[1:] {
				n = n*10 + int(ch-'0')
			}
			var cmd []string
			for j := 0; j < n; j++ {
				if _, err := readLine(r); err != nil { // $len
					return
				}
				s, err := readLine(r)
				if err != nil {
					return
				}
				cmd = append(cmd, s)
			}
			switch cmd[0] {
			case "EVALSHA":
				_, _ = conn.Write([]byte("-NOSCRIPT No matching script\r\n"))
			case "EVAL":
				_, _ = conn.Write([]byte("*2\r\n:1\r\n$" + strconvItoa(len(cmd[1])) + "\r\n" + cmd[1] + "\r\n"))
			default:
				_, _ = conn.Write([]byte("-ERR unexpected " + cmd[0] + "\r\n"))
			}
		}
	}()
	c, _ := newRESPClient("redis://" + ln.Addr().String())
	v, err := c.eval("return 1", "deadbeef", []string{"k"}, "a")
	if err != nil {
		t.Fatal(err)
	}
	arr, _ := v.([]any)
	if len(arr) != 2 || arr[0] != int64(1) || arr[1] != "return 1" {
		t.Fatalf("eval reply = %v", v)
	}
}

func strconvItoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// A pooled connection can be dead after the store restarts. The client must
// skip it and use a fresh connection instead of reporting the store down.
func TestRESPClientSkipsAStalePooledConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan int, 4)
	go func() {
		for i := 1; ; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- i
			go func(conn net.Conn, first bool) {
				defer conn.Close()
				r := bufio.NewReader(conn)
				for {
					line, err := readLine(r) // *N
					if err != nil {
						return
					}
					n := 0
					for _, ch := range line[1:] {
						n = n*10 + int(ch-'0')
					}
					for j := 0; j < n; j++ {
						if _, err := readLine(r); err != nil {
							return
						}
						if _, err := readLine(r); err != nil {
							return
						}
					}
					_, _ = conn.Write([]byte("+PONG\r\n"))
					if first {
						return // the "restart": drop the connection after one reply
					}
				}
			}(conn, i == 1)
		}
	}()
	c, _ := newRESPClient("redis://" + ln.Addr().String())
	if v, err := c.do("PING"); err != nil || v != "PONG" {
		t.Fatalf("first ping: %v %v", v, err)
	}
	// The pooled connection is now closed server-side; the next command must
	// still succeed, on a fresh connection.
	if v, err := c.do("PING"); err != nil || v != "PONG" {
		t.Fatalf("ping after the store restarted: %v %v", v, err)
	}
	if len(accepted) != 2 {
		t.Fatalf("want 2 connections (one stale, one fresh), got %d", len(accepted))
	}
}

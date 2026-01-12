package server

import (
	"bufio"
	"io"
	"log"
	"net"
	"time"

	"github.com/mevdschee/tqsession/pkg/tqsession"
)

// Server represents the TQSession network server.
type Server struct {
	cache *tqsession.Cache
	addr  string
}

// New creates a new Server instance.
func New(cache *tqsession.Cache, addr string) *Server {
	return &Server{
		cache: cache,
		addr:  addr,
	}
}

// Start runs the TCP server.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	log.Printf("Listening on %s", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("Accepted connection from %s", conn.RemoteAddr())

	// Peek first byte to determine protocol
	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	firstByte, err := reader.Peek(1)
	if err != nil {
		if err != io.EOF {
			log.Printf("Peek error from %s: %v", conn.RemoteAddr(), err)
		} else {
			log.Printf("Connection closed during peek %s", conn.RemoteAddr())
		}
		return
	}
	conn.SetReadDeadline(time.Time{}) // Reset deadline

	log.Printf("First byte: 0x%x (Binary: %v)", firstByte[0], firstByte[0] == 0x80)

	// Use buffered writer for all responses
	writer := bufio.NewWriter(conn)

	if firstByte[0] == 0x80 {
		s.handleBinary(conn, reader, writer)
	} else {
		s.handleText(reader, writer)
	}
}

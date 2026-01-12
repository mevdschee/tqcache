package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func (s *Server) handleText(reader *bufio.Reader, writer *bufio.Writer) {
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %v", err)
			}
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "SET":
			s.handleTextStorage(reader, writer, parts, "SET")
		case "ADD":
			s.handleTextStorage(reader, writer, parts, "ADD")
		case "REPLACE":
			s.handleTextStorage(reader, writer, parts, "REPLACE")
		case "CAS":
			s.handleTextCas(reader, writer, parts)
		case "GET":
			s.handleTextGet(writer, parts, false)
		case "GETS":
			s.handleTextGet(writer, parts, true)
		case "DELETE":
			s.handleTextDelete(writer, parts)
		case "INCR":
			s.handleTextIncrDecr(writer, parts, true)
		case "DECR":
			s.handleTextIncrDecr(writer, parts, false)
		case "TOUCH":
			s.handleTextTouch(writer, parts)
		case "FLUSH_ALL":
			s.handleTextFlushAll(writer, parts)
		case "QUIT":
			return
		case "VERSION":
			writer.WriteString("VERSION 1.0.0\r\n")
			writer.Flush()
		case "STATS":
			s.handleTextStats(writer)
		default:
			writer.WriteString("ERROR\r\n")
			writer.Flush()
		}
	}
}

func (s *Server) handleTextStorage(reader *bufio.Reader, writer *bufio.Writer, parts []string, op string) {
	if len(parts) < 5 {
		writer.WriteString("CLIENT_ERROR bad data chunk\r\n")
		writer.Flush()
		return
	}

	key := parts[1]
	exptime, _ := strconv.ParseInt(parts[3], 10, 64)
	bytes, _ := strconv.Atoi(parts[4])
	noreply := len(parts) > 5 && parts[5] == "noreply"

	// Read value
	value := make([]byte, bytes)
	if _, err := io.ReadFull(reader, value); err != nil {
		writer.WriteString("SERVER_ERROR read error\r\n")
		writer.Flush()
		return
	}

	// Read \r\n
	c, _ := reader.ReadByte()
	if c == '\r' {
		reader.ReadByte()
	}

	// Calculate TTL
	var ttl time.Duration
	if exptime > 0 {
		if exptime > 2592000 {
			ttl = time.Until(time.Unix(exptime, 0))
		} else {
			ttl = time.Duration(exptime) * time.Second
		}
	}

	var err error
	switch op {
	case "SET":
		_, err = s.cache.Set(key, value, ttl)
	case "ADD":
		_, err = s.cache.Add(key, value, ttl)
	case "REPLACE":
		_, err = s.cache.Replace(key, value, ttl)
	}

	if err != nil {
		if err == os.ErrExist || err == os.ErrNotExist {
			if !noreply {
				writer.WriteString("NOT_STORED\r\n")
				writer.Flush()
			}
			return
		}
		writer.WriteString("SERVER_ERROR " + err.Error() + "\r\n")
		writer.Flush()
		return
	}

	if !noreply {
		writer.WriteString("STORED\r\n")
		writer.Flush()
	}
}

func (s *Server) handleTextCas(reader *bufio.Reader, writer *bufio.Writer, parts []string) {
	if len(parts) < 6 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		writer.Flush()
		return
	}

	key := parts[1]
	exptime, _ := strconv.ParseInt(parts[3], 10, 64)
	bytes, _ := strconv.Atoi(parts[4])
	casToken, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		writer.Flush()
		return
	}
	noreply := len(parts) > 6 && parts[6] == "noreply"

	// Read value
	value := make([]byte, bytes)
	if _, err := io.ReadFull(reader, value); err != nil {
		writer.WriteString("SERVER_ERROR read error\r\n")
		writer.Flush()
		return
	}

	// Read \r\n
	c, _ := reader.ReadByte()
	if c == '\r' {
		reader.ReadByte()
	}

	// Calculate TTL
	var ttl time.Duration
	if exptime > 0 {
		if exptime > 2592000 {
			ttl = time.Until(time.Unix(exptime, 0))
		} else {
			ttl = time.Duration(exptime) * time.Second
		}
	}

	_, err = s.cache.Cas(key, value, ttl, casToken)
	if err != nil {
		if err == os.ErrExist {
			if !noreply {
				writer.WriteString("EXISTS\r\n")
				writer.Flush()
			}
			return
		}
		if err == os.ErrNotExist {
			if !noreply {
				writer.WriteString("NOT_FOUND\r\n")
				writer.Flush()
			}
			return
		}
		writer.WriteString("SERVER_ERROR " + err.Error() + "\r\n")
		writer.Flush()
		return
	}

	if !noreply {
		writer.WriteString("STORED\r\n")
		writer.Flush()
	}
}

func (s *Server) handleTextGet(writer *bufio.Writer, parts []string, withCas bool) {
	if len(parts) < 2 {
		writer.WriteString("ERROR\r\n")
		writer.Flush()
		return
	}

	for _, key := range parts[1:] {
		value, cas, err := s.cache.Get(key)
		if err == nil {
			writer.WriteString("VALUE ")
			writer.WriteString(key)
			writer.WriteString(" 0 ")
			writer.WriteString(strconv.Itoa(len(value)))
			if withCas {
				writer.WriteString(" ")
				writer.WriteString(strconv.FormatUint(cas, 10))
			}
			writer.WriteString("\r\n")
			writer.Write(value)
			writer.WriteString("\r\n")
		}
	}
	writer.WriteString("END\r\n")
	writer.Flush()
}

func (s *Server) handleTextDelete(writer *bufio.Writer, parts []string) {
	if len(parts) < 2 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		writer.Flush()
		return
	}
	key := parts[1]
	noreply := len(parts) > 2 && parts[2] == "noreply"

	err := s.cache.Delete(key)
	if err == nil {
		if !noreply {
			writer.WriteString("DELETED\r\n")
			writer.Flush()
		}
	} else {
		if !noreply {
			writer.WriteString("NOT_FOUND\r\n")
			writer.Flush()
		}
	}
}

func (s *Server) handleTextIncrDecr(writer *bufio.Writer, parts []string, incr bool) {
	if len(parts) < 3 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		writer.Flush()
		return
	}
	key := parts[1]
	valStr := parts[2]
	delta, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR invalid numeric delta argument\r\n")
		writer.Flush()
		return
	}
	noreply := len(parts) > 3 && parts[3] == "noreply"

	var newVal uint64
	if incr {
		newVal, _, err = s.cache.Increment(key, delta)
	} else {
		newVal, _, err = s.cache.Decrement(key, delta)
	}

	if err != nil {
		if err == os.ErrNotExist {
			if !noreply {
				writer.WriteString("NOT_FOUND\r\n")
				writer.Flush()
			}
			return
		}
		writer.WriteString("CLIENT_ERROR " + err.Error() + "\r\n")
		writer.Flush()
		return
	}

	if !noreply {
		writer.WriteString(strconv.FormatUint(newVal, 10) + "\r\n")
		writer.Flush()
	}
}

func (s *Server) handleTextTouch(writer *bufio.Writer, parts []string) {
	if len(parts) < 3 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		writer.Flush()
		return
	}

	key := parts[1]
	exptime, _ := strconv.ParseInt(parts[2], 10, 64)
	noreply := len(parts) > 3 && parts[3] == "noreply"

	var ttl time.Duration
	if exptime > 0 {
		if exptime > 2592000 {
			ttl = time.Until(time.Unix(exptime, 0))
		} else {
			ttl = time.Duration(exptime) * time.Second
		}
	}

	_, err := s.cache.Touch(key, ttl)
	if err != nil {
		if !noreply {
			if err == os.ErrNotExist {
				writer.WriteString("NOT_FOUND\r\n")
			} else {
				writer.WriteString("SERVER_ERROR " + err.Error() + "\r\n")
			}
			writer.Flush()
		}
		return
	}

	if !noreply {
		writer.WriteString("TOUCHED\r\n")
		writer.Flush()
	}
}

func (s *Server) handleTextFlushAll(writer *bufio.Writer, parts []string) {
	noreply := false
	for _, p := range parts[1:] {
		if p == "noreply" {
			noreply = true
		}
	}

	s.cache.FlushAll()
	if !noreply {
		writer.WriteString("OK\r\n")
		writer.Flush()
	}
}

func (s *Server) handleTextStats(writer *bufio.Writer) {
	stats := s.cache.Stats()
	writer.WriteString(fmt.Sprintf("STAT pid %d\r\n", os.Getpid()))
	writer.WriteString(fmt.Sprintf("STAT uptime %d\r\n", int64(time.Since(s.cache.GetStartTime()).Seconds())))
	writer.WriteString(fmt.Sprintf("STAT time %d\r\n", time.Now().Unix()))
	writer.WriteString("STAT version 1.0.0\r\n")
	for k, v := range stats {
		writer.WriteString(fmt.Sprintf("STAT %s %s\r\n", k, v))
	}
	writer.WriteString("END\r\n")
	writer.Flush()
}

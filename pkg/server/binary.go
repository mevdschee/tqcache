package server

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/mevdschee/tqcache/pkg/tqcache"
)

const (
	reqMagic = 0x80
	resMagic = 0x81

	opGet       = 0x00
	opSet       = 0x01
	opAdd       = 0x02
	opReplace   = 0x03
	opDelete    = 0x04
	opIncrement = 0x05
	opDecrement = 0x06
	opQuit      = 0x07
	opFlush     = 0x08
	opGetQ      = 0x09
	opNoop      = 0x0a
	opVersion   = 0x0b
	opGetK      = 0x0c
	opGetKQ     = 0x0d
	opAppend    = 0x0e
	opPrepend   = 0x0f
	opStat      = 0x10
	opTouch     = 0x1c
	opGAT       = 0x1d
	opGATK      = 0x1e
)

const (
	resSuccess       = 0x0000
	resKeyNotFound   = 0x0001
	resKeyExists     = 0x0002
	resValueTooLarge = 0x0003
	resInvalidArgs   = 0x0004
	resItemNotStored = 0x0005
	resUnknownCmd    = 0x0081
	resOOM           = 0x0082
)

type binaryHeader struct {
	Magic    uint8
	Opcode   uint8
	KeyLen   uint16
	ExtraLen uint8
	DataType uint8
	VBucket  uint16
	BodyLen  uint32
	Opaque   uint32
	CAS      uint64
}

func (s *Server) handleBinary(conn net.Conn, reader *bufio.Reader, writer *bufio.Writer) {
	headerBuf := make([]byte, 24)

	for {
		if _, err := io.ReadFull(reader, headerBuf); err != nil {
			if err != io.EOF {
				log.Printf("Binary read header error: %v", err)
			}
			return
		}

		if headerBuf[0] != reqMagic {
			log.Printf("Invalid magic byte: %x", headerBuf[0])
			return
		}

		req := binaryHeader{
			Magic:    headerBuf[0],
			Opcode:   headerBuf[1],
			KeyLen:   binary.BigEndian.Uint16(headerBuf[2:4]),
			ExtraLen: headerBuf[4],
			DataType: headerBuf[5],
			VBucket:  binary.BigEndian.Uint16(headerBuf[6:8]),
			BodyLen:  binary.BigEndian.Uint32(headerBuf[8:12]),
			Opaque:   binary.BigEndian.Uint32(headerBuf[12:16]),
			CAS:      binary.BigEndian.Uint64(headerBuf[16:24]),
		}

		bodyBuf := make([]byte, req.BodyLen)
		if _, err := io.ReadFull(reader, bodyBuf); err != nil {
			log.Printf("Binary read body error: %v", err)
			return
		}

		extras := bodyBuf[:req.ExtraLen]
		key := string(bodyBuf[req.ExtraLen : uint32(req.ExtraLen)+uint32(req.KeyLen)])
		value := bodyBuf[uint32(req.ExtraLen)+uint32(req.KeyLen):]

		switch req.Opcode {
		case opSet:
			s.handleBinaryStorage(writer, req, extras, key, value, "SET")
		case opAdd:
			s.handleBinaryStorage(writer, req, extras, key, value, "ADD")
		case opReplace:
			s.handleBinaryStorage(writer, req, extras, key, value, "REPLACE")
		case opDelete:
			s.handleBinaryDelete(writer, req, key)
		case opIncrement:
			s.handleBinaryIncrDecr(writer, req, extras, key, true)
		case opDecrement:
			s.handleBinaryIncrDecr(writer, req, extras, key, false)
		case opFlush:
			s.handleBinaryFlush(writer, req)
		case opGet:
			s.handleBinaryGet(writer, req, key, false)
		case opGetQ:
			s.handleBinaryGet(writer, req, key, true)
		case opGetK:
			s.handleBinaryGetK(writer, req, key, false)
		case opGetKQ:
			s.handleBinaryGetK(writer, req, key, true)
		case opVersion:
			s.handleBinaryVersion(writer, req)
		case opQuit:
			return
		case opNoop:
			s.sendBinaryResponse(writer, req, resSuccess, nil, nil, nil, 0)
		case opAppend:
			s.handleBinaryAppendPrepend(writer, req, key, value, true)
		case opPrepend:
			s.handleBinaryAppendPrepend(writer, req, key, value, false)
		case opStat:
			s.handleBinaryStats(writer, req)
		case opTouch:
			s.handleBinaryTouch(writer, req, extras, key)
		case opGAT:
			s.handleBinaryGAT(writer, req, extras, key)
		case opGATK:
			s.handleBinaryGATK(writer, req, extras, key)
		default:
			log.Printf("Binary Unknown Opcode: 0x%02x", req.Opcode)
			s.sendBinaryResponse(writer, req, resUnknownCmd, nil, nil, nil, 0)
		}

		writer.Flush()
	}
}

func (s *Server) handleBinaryStorage(writer *bufio.Writer, req binaryHeader, extras []byte, key string, value []byte, op string) {
	if len(extras) != 8 {
		s.sendBinaryResponse(writer, req, resInvalidArgs, nil, nil, nil, 0)
		return
	}

	expiry := binary.BigEndian.Uint32(extras[4:8])

	var ttl time.Duration
	if expiry > 0 {
		if expiry > 2592000 {
			ttl = time.Until(time.Unix(int64(expiry), 0))
		} else {
			ttl = time.Duration(expiry) * time.Second
		}
	}

	var err error
	var newCas uint64
	if req.CAS > 0 {
		newCas, err = s.cache.Cas(key, value, ttl, req.CAS)
	} else {
		switch op {
		case "SET":
			newCas, err = s.cache.Set(key, value, ttl)
		case "ADD":
			newCas, err = s.cache.Add(key, value, ttl)
		case "REPLACE":
			newCas, err = s.cache.Replace(key, value, ttl)
		}
	}

	if err != nil {
		if err == tqcache.ErrValueTooLarge {
			s.sendBinaryResponse(writer, req, resValueTooLarge, nil, nil, nil, 0)
			return
		}
		if err == os.ErrExist {
			s.sendBinaryResponse(writer, req, resKeyExists, nil, nil, nil, 0)
			return
		}
		if err == os.ErrNotExist {
			s.sendBinaryResponse(writer, req, resKeyNotFound, nil, nil, nil, 0)
			return
		}
		s.sendBinaryResponse(writer, req, resItemNotStored, nil, nil, nil, 0)
		return
	}

	s.sendBinaryResponse(writer, req, resSuccess, nil, nil, nil, newCas)
}

func (s *Server) handleBinaryGet(writer *bufio.Writer, req binaryHeader, key string, quiet bool) {
	val, cas, err := s.cache.Get(key)
	if err != nil {
		if quiet {
			return
		}
		s.sendBinaryResponse(writer, req, resKeyNotFound, nil, nil, nil, 0)
		return
	}

	extras := make([]byte, 4)
	s.sendBinaryResponse(writer, req, resSuccess, extras, nil, val, cas)
}

func (s *Server) handleBinaryGetK(writer *bufio.Writer, req binaryHeader, key string, quiet bool) {
	val, cas, err := s.cache.Get(key)
	if err != nil {
		if quiet {
			return
		}
		s.sendBinaryResponse(writer, req, resKeyNotFound, nil, nil, nil, 0)
		return
	}
	extras := make([]byte, 4)
	s.sendBinaryResponse(writer, req, resSuccess, extras, []byte(key), val, cas)
}

func (s *Server) handleBinaryDelete(writer *bufio.Writer, req binaryHeader, key string) {
	err := s.cache.Delete(key)
	if err == nil {
		s.sendBinaryResponse(writer, req, resSuccess, nil, nil, nil, 0)
	} else {
		s.sendBinaryResponse(writer, req, resKeyNotFound, nil, nil, nil, 0)
	}
}

func (s *Server) handleBinaryIncrDecr(writer *bufio.Writer, req binaryHeader, extras []byte, key string, incr bool) {
	if len(extras) < 20 {
		s.sendBinaryResponse(writer, req, resInvalidArgs, nil, nil, nil, 0)
		return
	}

	delta := binary.BigEndian.Uint64(extras[0:8])
	initial := binary.BigEndian.Uint64(extras[8:16])
	expiry := binary.BigEndian.Uint32(extras[16:20])

	var newVal uint64
	var cas uint64
	var err error

	if incr {
		newVal, cas, err = s.cache.Increment(key, delta)
	} else {
		newVal, cas, err = s.cache.Decrement(key, delta)
	}

	if err == os.ErrNotExist {
		if expiry == 0xFFFFFFFF {
			s.sendBinaryResponse(writer, req, resKeyNotFound, nil, nil, nil, 0)
			return
		}

		var ttl time.Duration
		if expiry > 0 {
			if expiry > 2592000 {
				ttl = time.Until(time.Unix(int64(expiry), 0))
			} else {
				ttl = time.Duration(expiry) * time.Second
			}
		}

		initS := strconv.FormatUint(initial, 10)
		cas, err = s.cache.Set(key, []byte(initS), ttl)
		if err != nil {
			s.sendBinaryResponse(writer, req, resItemNotStored, nil, nil, nil, 0)
			return
		}
		newVal = initial
	} else if err != nil {
		s.sendBinaryResponse(writer, req, resInvalidArgs, nil, nil, nil, 0)
		return
	}

	resBody := make([]byte, 8)
	binary.BigEndian.PutUint64(resBody, newVal)
	s.sendBinaryResponse(writer, req, resSuccess, nil, nil, resBody, cas)
}

func (s *Server) handleBinaryFlush(writer *bufio.Writer, req binaryHeader) {
	s.cache.FlushAll()
	s.sendBinaryResponse(writer, req, resSuccess, nil, nil, nil, 0)
}

func (s *Server) handleBinaryAppendPrepend(writer *bufio.Writer, req binaryHeader, key string, value []byte, isAppend bool) {
	if req.ExtraLen != 0 {
		s.sendBinaryResponse(writer, req, resInvalidArgs, nil, nil, nil, 0)
		return
	}

	var err error
	var cas uint64
	if isAppend {
		cas, err = s.cache.Append(key, value)
	} else {
		cas, err = s.cache.Prepend(key, value)
	}

	if err != nil {
		if err == tqcache.ErrValueTooLarge {
			s.sendBinaryResponse(writer, req, resValueTooLarge, nil, nil, nil, 0)
			return
		}
		if err == os.ErrNotExist {
			s.sendBinaryResponse(writer, req, resItemNotStored, nil, nil, nil, 0)
			return
		}
		s.sendBinaryResponse(writer, req, resItemNotStored, nil, nil, nil, 0)
		return
	}

	s.sendBinaryResponse(writer, req, resSuccess, nil, nil, nil, cas)
}

func (s *Server) handleBinaryVersion(writer *bufio.Writer, req binaryHeader) {
	s.sendBinaryResponse(writer, req, resSuccess, nil, nil, []byte("1.0.0"), 0)
}

func (s *Server) handleBinaryStats(writer *bufio.Writer, req binaryHeader) {
	stats := s.cache.Stats()
	for k, v := range stats {
		s.sendBinaryResponse(writer, req, resSuccess, nil, []byte(k), []byte(v), 0)
	}
	s.sendBinaryResponse(writer, req, resSuccess, nil, nil, nil, 0)
}

func (s *Server) handleBinaryTouch(writer *bufio.Writer, req binaryHeader, extras []byte, key string) {
	if len(extras) != 4 {
		s.sendBinaryResponse(writer, req, resInvalidArgs, nil, nil, nil, 0)
		return
	}
	expiry := binary.BigEndian.Uint32(extras[0:4])
	var ttl time.Duration
	if expiry > 0 {
		if expiry > 2592000 {
			ttl = time.Until(time.Unix(int64(expiry), 0))
		} else {
			ttl = time.Duration(expiry) * time.Second
		}
	}

	cas, err := s.cache.Touch(key, ttl)
	if err != nil {
		s.sendBinaryResponse(writer, req, resKeyNotFound, nil, nil, nil, 0)
		return
	}
	s.sendBinaryResponse(writer, req, resSuccess, nil, nil, nil, cas)
}

func (s *Server) handleBinaryGAT(writer *bufio.Writer, req binaryHeader, extras []byte, key string) {
	s.handleBinaryGATCommon(writer, req, extras, key, false)
}

func (s *Server) handleBinaryGATK(writer *bufio.Writer, req binaryHeader, extras []byte, key string) {
	s.handleBinaryGATCommon(writer, req, extras, key, true)
}

func (s *Server) handleBinaryGATCommon(writer *bufio.Writer, req binaryHeader, extras []byte, key string, returnKey bool) {
	if len(extras) != 4 {
		s.sendBinaryResponse(writer, req, resInvalidArgs, nil, nil, nil, 0)
		return
	}
	expiry := binary.BigEndian.Uint32(extras[0:4])
	var ttl time.Duration
	if expiry > 0 {
		if expiry > 2592000 {
			ttl = time.Until(time.Unix(int64(expiry), 0))
		} else {
			ttl = time.Duration(expiry) * time.Second
		}
	}

	cas, err := s.cache.Touch(key, ttl)
	if err != nil {
		s.sendBinaryResponse(writer, req, resKeyNotFound, nil, nil, nil, 0)
		return
	}

	val, _, err := s.cache.Get(key)
	if err != nil {
		s.sendBinaryResponse(writer, req, resKeyNotFound, nil, nil, nil, 0)
		return
	}

	resExtras := make([]byte, 4)
	var keyBytes []byte
	if returnKey {
		keyBytes = []byte(key)
	}

	s.sendBinaryResponse(writer, req, resSuccess, resExtras, keyBytes, val, cas)
}

func (s *Server) sendBinaryResponse(writer *bufio.Writer, req binaryHeader, status uint16, extras []byte, key []byte, value []byte, cas uint64) {
	totalBodyLen := uint32(len(extras) + len(key) + len(value))
	totalLen := 24 + totalBodyLen
	buf := make([]byte, totalLen)

	buf[0] = resMagic
	buf[1] = req.Opcode
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(key)))
	buf[4] = uint8(len(extras))
	buf[5] = 0
	binary.BigEndian.PutUint16(buf[6:8], status)
	binary.BigEndian.PutUint32(buf[8:12], totalBodyLen)
	binary.BigEndian.PutUint32(buf[12:16], req.Opaque)
	binary.BigEndian.PutUint64(buf[16:24], cas)

	if len(extras) > 0 {
		copy(buf[24:], extras)
	}
	if len(key) > 0 {
		copy(buf[24+len(extras):], key)
	}
	if len(value) > 0 {
		copy(buf[24+len(extras)+len(key):], value)
	}

	if _, err := writer.Write(buf); err != nil {
		log.Printf("Response Write Error: %v", err)
	}
}

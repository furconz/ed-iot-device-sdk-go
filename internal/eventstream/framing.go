package eventstream

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// EventStream message wire format:
// [Prelude] [Headers] [Payload] [Message CRC]
//
// Prelude (12 bytes):
//   - Total Length (4 bytes, big-endian uint32) - includes prelude
//   - Headers Length (4 bytes, big-endian uint32)
//   - Prelude CRC (4 bytes, big-endian uint32) - CRC32 of first 8 bytes
//
// Headers: Variable length, encoded as:
//   - Header Name Length (1 byte)
//   - Header Name (variable, UTF-8)
//   - Header Value Type (1 byte)
//   - Header Value (variable, depends on type)
//
// Payload: Variable length raw bytes
//
// Message CRC (4 bytes): CRC32 of entire message excluding this field

const (
	preludeLength       = 12
	preludeCRCOffset    = 8
	minMessageLength    = preludeLength + 4 // Prelude + Message CRC
	maxHeaderNameLength = 255
)

// EncodeMessage encodes a Message into wire format
func EncodeMessage(msg *Message) ([]byte, error) {
	// Encode headers (protocol headers first, then user headers)
	headersBuf, err := encodeHeaders(msg.Headers, msg.Type, msg.Flags)
	if err != nil {
		return nil, fmt.Errorf("failed to encode headers: %w", err)
	}

	headersLen := len(headersBuf)
	payloadLen := len(msg.Payload)
	totalLen := preludeLength + headersLen + payloadLen + 4 // +4 for message CRC

	buf := make([]byte, totalLen)
	offset := 0

	// Write total length
	binary.BigEndian.PutUint32(buf[offset:], uint32(totalLen))
	offset += 4

	// Write headers length
	binary.BigEndian.PutUint32(buf[offset:], uint32(headersLen))
	offset += 4

	// Calculate and write prelude CRC
	preludeCRC := crc32.ChecksumIEEE(buf[0:8])
	binary.BigEndian.PutUint32(buf[offset:], preludeCRC)
	offset += 4

	// Write headers
	copy(buf[offset:], headersBuf)
	offset += headersLen

	// Write payload
	copy(buf[offset:], msg.Payload)
	offset += payloadLen

	// Calculate and write message CRC (over everything except the CRC itself)
	messageCRC := crc32.ChecksumIEEE(buf[0:offset])
	binary.BigEndian.PutUint32(buf[offset:], messageCRC)

	return buf, nil
}

// DecodeMessage decodes a Message from wire format
func DecodeMessage(reader io.Reader) (*Message, error) {
	// Read prelude
	prelude := make([]byte, preludeLength)
	if _, err := io.ReadFull(reader, prelude); err != nil {
		return nil, fmt.Errorf("failed to read prelude: %w", err)
	}

	// Verify prelude CRC
	preludeCRC := binary.BigEndian.Uint32(prelude[preludeCRCOffset:])
	expectedCRC := crc32.ChecksumIEEE(prelude[0:8])
	if preludeCRC != expectedCRC {
		return nil, fmt.Errorf("prelude CRC mismatch: got 0x%x, expected 0x%x", preludeCRC, expectedCRC)
	}

	// Extract lengths
	totalLen := binary.BigEndian.Uint32(prelude[0:4])
	headersLen := binary.BigEndian.Uint32(prelude[4:8])

	if totalLen < minMessageLength {
		return nil, fmt.Errorf("invalid total length: %d (minimum %d)", totalLen, minMessageLength)
	}

	// Calculate payload length
	payloadLen := totalLen - preludeLength - headersLen - 4

	// Read headers
	headersBuf := make([]byte, headersLen)
	if headersLen > 0 {
		if _, err := io.ReadFull(reader, headersBuf); err != nil {
			return nil, fmt.Errorf("failed to read headers: %w", err)
		}
	}

	// Read payload
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, fmt.Errorf("failed to read payload: %w", err)
		}
	}

	// Read and verify message CRC
	messageCRCBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, messageCRCBuf); err != nil {
		return nil, fmt.Errorf("failed to read message CRC: %w", err)
	}
	messageCRC := binary.BigEndian.Uint32(messageCRCBuf)

	// Verify message CRC
	crcBuf := make([]byte, totalLen-4)
	copy(crcBuf[0:], prelude)
	copy(crcBuf[preludeLength:], headersBuf)
	copy(crcBuf[preludeLength+headersLen:], payload)
	expectedMessageCRC := crc32.ChecksumIEEE(crcBuf)
	if messageCRC != expectedMessageCRC {
		return nil, fmt.Errorf("message CRC mismatch: got 0x%x, expected 0x%x", messageCRC, expectedMessageCRC)
	}

	// Decode headers
	headers, msgType, msgFlags, err := decodeHeaders(headersBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to decode headers: %w", err)
	}

	return &Message{
		Type:    msgType,
		Flags:   msgFlags,
		Headers: headers,
		Payload: payload,
	}, nil
}

// encodeHeaders encodes headers into wire format
// CRITICAL: Protocol headers (:message-type, :message-flags, :stream-id) MUST be encoded FIRST,
// then user headers. This matches the AWS CRT implementation and is required by the EventStream RPC protocol.
func encodeHeaders(headers []Header, msgType MessageType, msgFlags MessageFlags) ([]byte, error) {
	buf := &bytes.Buffer{}

	// Extract stream-id from headers before encoding
	var streamID int32 = 0
	for _, h := range headers {
		if h.Name == ":stream-id" {
			if v, ok := h.Value.(int32); ok {
				streamID = v
			}
		}
	}

	// FIRST: Encode protocol headers - these MUST come first!
	if err := encodeHeader(buf, Header{Name: ":message-type", Type: HeaderTypeInt32, Value: int32(msgType)}); err != nil {
		return nil, err
	}
	if err := encodeHeader(buf, Header{Name: ":message-flags", Type: HeaderTypeInt32, Value: int32(msgFlags)}); err != nil {
		return nil, err
	}
	if err := encodeHeader(buf, Header{Name: ":stream-id", Type: HeaderTypeInt32, Value: streamID}); err != nil {
		return nil, err
	}

	// SECOND: Encode user headers (non-protocol headers)
	for _, h := range headers {
		// Skip protocol headers if they somehow ended up in the user headers
		if h.Name == ":message-type" || h.Name == ":message-flags" || h.Name == ":stream-id" {
			continue
		}
		if err := encodeHeader(buf, h); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// encodeHeader encodes a single header
func encodeHeader(buf *bytes.Buffer, h Header) error {
	nameBytes := []byte(h.Name)
	if len(nameBytes) > maxHeaderNameLength {
		return fmt.Errorf("header name too long: %d bytes (max %d)", len(nameBytes), maxHeaderNameLength)
	}

	// Write name length
	buf.WriteByte(byte(len(nameBytes)))

	// Write name
	buf.Write(nameBytes)

	// Write header type
	buf.WriteByte(byte(h.Type))

	// Write value based on type
	switch h.Type {
	case HeaderTypeBoolTrue, HeaderTypeBoolFalse:
		// Boolean values are encoded in the type byte itself

	case HeaderTypeByte:
		v, ok := h.Value.(byte)
		if !ok {
			return fmt.Errorf("header %s: expected byte value", h.Name)
		}
		buf.WriteByte(v)

	case HeaderTypeInt16:
		v, ok := h.Value.(int16)
		if !ok {
			return fmt.Errorf("header %s: expected int16 value", h.Name)
		}
		binary.Write(buf, binary.BigEndian, v)

	case HeaderTypeInt32:
		v, ok := h.Value.(int32)
		if !ok {
			// Try uint32 for message-type and message-flags
			if vuint, ok := h.Value.(uint32); ok {
				v = int32(vuint)
			} else {
				return fmt.Errorf("header %s: expected int32 value", h.Name)
			}
		}
		binary.Write(buf, binary.BigEndian, v)

	case HeaderTypeInt64:
		v, ok := h.Value.(int64)
		if !ok {
			return fmt.Errorf("header %s: expected int64 value", h.Name)
		}
		binary.Write(buf, binary.BigEndian, v)

	case HeaderTypeByteArray:
		v, ok := h.Value.([]byte)
		if !ok {
			return fmt.Errorf("header %s: expected []byte value", h.Name)
		}
		binary.Write(buf, binary.BigEndian, uint16(len(v)))
		buf.Write(v)

	case HeaderTypeString:
		v, ok := h.Value.(string)
		if !ok {
			return fmt.Errorf("header %s: expected string value", h.Name)
		}
		vBytes := []byte(v)
		binary.Write(buf, binary.BigEndian, uint16(len(vBytes)))
		buf.Write(vBytes)

	case HeaderTypeTimestamp:
		v, ok := h.Value.(int64)
		if !ok {
			return fmt.Errorf("header %s: expected int64 timestamp value", h.Name)
		}
		binary.Write(buf, binary.BigEndian, v)

	case HeaderTypeUUID:
		v, ok := h.Value.([16]byte)
		if !ok {
			return fmt.Errorf("header %s: expected [16]byte UUID value", h.Name)
		}
		buf.Write(v[:])

	default:
		return fmt.Errorf("unsupported header type: %d", h.Type)
	}

	return nil
}

// decodeHeaders decodes headers from wire format
func decodeHeaders(data []byte) ([]Header, MessageType, MessageFlags, error) {
	buf := bytes.NewReader(data)
	headers := []Header{}
	var msgType MessageType
	var msgFlags MessageFlags

	for buf.Len() > 0 {
		h, err := decodeHeader(buf)
		if err != nil {
			return nil, 0, 0, err
		}

		// Extract message-type and message-flags from special headers
		if h.Name == ":message-type" {
			if v, ok := h.Value.(int32); ok {
				msgType = MessageType(v)
			}
		} else if h.Name == ":message-flags" {
			if v, ok := h.Value.(int32); ok {
				msgFlags = MessageFlags(v)
			}
		} else {
			headers = append(headers, h)
		}
	}

	return headers, msgType, msgFlags, nil
}

// decodeHeader decodes a single header
func decodeHeader(buf *bytes.Reader) (Header, error) {
	// Read name length
	nameLenByte, err := buf.ReadByte()
	if err != nil {
		return Header{}, fmt.Errorf("failed to read header name length: %w", err)
	}
	nameLen := int(nameLenByte)

	// Read name
	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(buf, nameBytes); err != nil {
		return Header{}, fmt.Errorf("failed to read header name: %w", err)
	}
	name := string(nameBytes)

	// Read type
	typeByte, err := buf.ReadByte()
	if err != nil {
		return Header{}, fmt.Errorf("failed to read header type: %w", err)
	}
	headerType := HeaderType(typeByte)

	// Read value based on type
	var value interface{}
	switch headerType {
	case HeaderTypeBoolTrue:
		value = true

	case HeaderTypeBoolFalse:
		value = false

	case HeaderTypeByte:
		v, err := buf.ReadByte()
		if err != nil {
			return Header{}, fmt.Errorf("failed to read byte value: %w", err)
		}
		value = v

	case HeaderTypeInt16:
		var v int16
		if err := binary.Read(buf, binary.BigEndian, &v); err != nil {
			return Header{}, fmt.Errorf("failed to read int16 value: %w", err)
		}
		value = v

	case HeaderTypeInt32:
		var v int32
		if err := binary.Read(buf, binary.BigEndian, &v); err != nil {
			return Header{}, fmt.Errorf("failed to read int32 value: %w", err)
		}
		value = v

	case HeaderTypeInt64:
		var v int64
		if err := binary.Read(buf, binary.BigEndian, &v); err != nil {
			return Header{}, fmt.Errorf("failed to read int64 value: %w", err)
		}
		value = v

	case HeaderTypeByteArray:
		var length uint16
		if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
			return Header{}, fmt.Errorf("failed to read byte array length: %w", err)
		}
		v := make([]byte, length)
		if _, err := io.ReadFull(buf, v); err != nil {
			return Header{}, fmt.Errorf("failed to read byte array: %w", err)
		}
		value = v

	case HeaderTypeString:
		var length uint16
		if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
			return Header{}, fmt.Errorf("failed to read string length: %w", err)
		}
		v := make([]byte, length)
		if _, err := io.ReadFull(buf, v); err != nil {
			return Header{}, fmt.Errorf("failed to read string: %w", err)
		}
		value = string(v)

	case HeaderTypeTimestamp:
		var v int64
		if err := binary.Read(buf, binary.BigEndian, &v); err != nil {
			return Header{}, fmt.Errorf("failed to read timestamp value: %w", err)
		}
		value = v

	case HeaderTypeUUID:
		var v [16]byte
		if _, err := io.ReadFull(buf, v[:]); err != nil {
			return Header{}, fmt.Errorf("failed to read UUID: %w", err)
		}
		value = v

	default:
		return Header{}, fmt.Errorf("unsupported header type: %d", headerType)
	}

	return Header{
		Name:  name,
		Type:  headerType,
		Value: value,
	}, nil
}

// CreateMessage is a helper to create a message with type and flags
// streamID is the stream identifier (0 for connection-level messages)
func CreateMessage(msgType MessageType, flags MessageFlags, payload []byte) *Message {
	return CreateMessageWithStreamID(msgType, flags, 0, payload)
}

// CreateMessageWithStreamID creates a message with a specific stream ID
func CreateMessageWithStreamID(msgType MessageType, flags MessageFlags, streamID uint32, payload []byte) *Message {
	msg := &Message{
		Type:    msgType,
		Flags:   flags,
		Payload: payload,
		// Store stream-id in headers - encodeHeaders will place all protocol headers first
		Headers: []Header{
			{
				Name:  ":stream-id",
				Type:  HeaderTypeInt32,
				Value: int32(streamID),
			},
		},
	}
	return msg
}

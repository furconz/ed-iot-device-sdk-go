package eventstream

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"
)

func preludeWith(totalLen, headersLen uint32) []byte {
	p := make([]byte, 12)
	binary.BigEndian.PutUint32(p[0:], totalLen)
	binary.BigEndian.PutUint32(p[4:], headersLen)
	binary.BigEndian.PutUint32(p[8:], crc32.ChecksumIEEE(p[0:8]))
	return p
}

func TestDecodeRejectsHeadersLenUnderflow(t *testing.T) {
	// headersLen > totalLen-16: pre-fix this underflows payloadLen to ~4 GiB
	// and attempts a giant allocation instead of failing cleanly.
	frame := preludeWith(100, 200)
	_, err := DecodeMessage(bytes.NewReader(frame))
	if err == nil || !strings.Contains(err.Error(), "invalid headers length") {
		t.Fatalf("want invalid-headers-length error, got %v", err)
	}
}

func TestDecodeRejectsOversizeMessage(t *testing.T) {
	frame := preludeWith(1<<31, 16)
	_, err := DecodeMessage(bytes.NewReader(frame))
	if err == nil || !strings.Contains(err.Error(), "invalid total length") {
		t.Fatalf("want invalid-total-length error, got %v", err)
	}
}

func TestDecodeRoundTripStillWorks(t *testing.T) {
	msg := CreateMessageWithStreamID(MessageTypeApplicationMessage, MessageFlagTerminateStream, 42, []byte(`{"a":1}`))
	msg.SetHeader("operation", HeaderTypeString, "aws.greengrass#GetConfiguration")
	b, err := EncodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeMessage(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != msg.Type || got.Flags != msg.Flags || string(got.Payload) != string(msg.Payload) {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

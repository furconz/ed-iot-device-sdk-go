package eventstream

import (
	"bytes"
	"testing"
)

func TestMessageRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		msg     *Message
		wantErr bool
	}{
		{
			name: "simple message with payload",
			msg: &Message{
				Type:    MessageTypeApplicationMessage,
				Flags:   MessageFlagNone,
				Payload: []byte("test payload"),
				Headers: []Header{
					{
						Name:  ":message-type",
						Type:  HeaderTypeInt32,
						Value: int32(MessageTypeApplicationMessage),
					},
					{
						Name:  ":message-flags",
						Type:  HeaderTypeInt32,
						Value: int32(MessageFlagNone),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "message with multiple headers",
			msg: &Message{
				Type:  MessageTypeApplicationMessage,
				Flags: MessageFlagNone,
				Headers: []Header{
					{
						Name:  ":message-type",
						Type:  HeaderTypeInt32,
						Value: int32(MessageTypeApplicationMessage),
					},
					{
						Name:  ":message-flags",
						Type:  HeaderTypeInt32,
						Value: int32(MessageFlagNone),
					},
					{
						Name:  "service-model-type",
						Type:  HeaderTypeString,
						Value: "aws.greengrass#GetConfigurationRequest",
					},
					{
						Name:  ":version",
						Type:  HeaderTypeString,
						Value: "0.1.0",
					},
				},
				Payload: []byte(`{"componentName":"my-component"}`),
			},
			wantErr: false,
		},
		{
			name: "empty payload",
			msg: &Message{
				Type:    MessageTypeConnect,
				Flags:   MessageFlagNone,
				Payload: []byte{},
				Headers: []Header{
					{
						Name:  ":message-type",
						Type:  HeaderTypeInt32,
						Value: int32(MessageTypeConnect),
					},
					{
						Name:  ":message-flags",
						Type:  HeaderTypeInt32,
						Value: int32(MessageFlagNone),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "message with flags",
			msg: &Message{
				Type:    MessageTypeConnectAck,
				Flags:   MessageFlagConnectionAccepted,
				Payload: []byte{},
				Headers: []Header{
					{
						Name:  ":message-type",
						Type:  HeaderTypeInt32,
						Value: int32(MessageTypeConnectAck),
					},
					{
						Name:  ":message-flags",
						Type:  HeaderTypeInt32,
						Value: int32(MessageFlagConnectionAccepted),
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded, err := EncodeMessage(tt.msg)
			if (err != nil) != tt.wantErr {
				t.Errorf("EncodeMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			// Decode
			decoded, err := DecodeMessage(bytes.NewReader(encoded))
			if err != nil {
				t.Errorf("DecodeMessage() error = %v", err)
				return
			}

			// Compare
			if decoded.Type != tt.msg.Type {
				t.Errorf("Type mismatch: got %v, want %v", decoded.Type, tt.msg.Type)
			}
			if decoded.Flags != tt.msg.Flags {
				t.Errorf("Flags mismatch: got %v, want %v", decoded.Flags, tt.msg.Flags)
			}
			if !bytes.Equal(decoded.Payload, tt.msg.Payload) {
				t.Errorf("Payload mismatch: got %v, want %v", decoded.Payload, tt.msg.Payload)
			}

			// Compare non-special headers (exclude :message-type and :message-flags)
			for _, origHeader := range tt.msg.Headers {
				if origHeader.Name == ":message-type" || origHeader.Name == ":message-flags" {
					continue
				}
				val, ok := decoded.GetHeader(origHeader.Name)
				if !ok {
					t.Errorf("Header %s not found in decoded message", origHeader.Name)
					continue
				}
				if val != origHeader.Value {
					t.Errorf("Header %s mismatch: got %v, want %v", origHeader.Name, val, origHeader.Value)
				}
			}
		})
	}
}

func TestHeaderTypes(t *testing.T) {
	tests := []struct {
		name   string
		header Header
	}{
		{
			name: "bool true",
			header: Header{
				Name:  "test-bool",
				Type:  HeaderTypeBoolTrue,
				Value: true,
			},
		},
		{
			name: "bool false",
			header: Header{
				Name:  "test-bool",
				Type:  HeaderTypeBoolFalse,
				Value: false,
			},
		},
		{
			name: "byte",
			header: Header{
				Name:  "test-byte",
				Type:  HeaderTypeByte,
				Value: byte(42),
			},
		},
		{
			name: "int16",
			header: Header{
				Name:  "test-int16",
				Type:  HeaderTypeInt16,
				Value: int16(-1234),
			},
		},
		{
			name: "int32",
			header: Header{
				Name:  "test-int32",
				Type:  HeaderTypeInt32,
				Value: int32(-123456),
			},
		},
		{
			name: "int64",
			header: Header{
				Name:  "test-int64",
				Type:  HeaderTypeInt64,
				Value: int64(-1234567890),
			},
		},
		{
			name: "byte array",
			header: Header{
				Name:  "test-bytes",
				Type:  HeaderTypeByteArray,
				Value: []byte{0x01, 0x02, 0x03, 0x04},
			},
		},
		{
			name: "string",
			header: Header{
				Name:  "test-string",
				Type:  HeaderTypeString,
				Value: "Hello, World!",
			},
		},
		{
			name: "timestamp",
			header: Header{
				Name:  "test-timestamp",
				Type:  HeaderTypeTimestamp,
				Value: int64(1609459200000), // 2021-01-01 00:00:00 UTC
			},
		},
		{
			name: "uuid",
			header: Header{
				Name:  "test-uuid",
				Type:  HeaderTypeUUID,
				Value: [16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Message{
				Type:    MessageTypeApplicationMessage,
				Flags:   MessageFlagNone,
				Payload: []byte("test"),
				Headers: []Header{
					{
						Name:  ":message-type",
						Type:  HeaderTypeInt32,
						Value: int32(MessageTypeApplicationMessage),
					},
					{
						Name:  ":message-flags",
						Type:  HeaderTypeInt32,
						Value: int32(MessageFlagNone),
					},
					tt.header,
				},
			}

			// Encode
			encoded, err := EncodeMessage(msg)
			if err != nil {
				t.Fatalf("EncodeMessage() error = %v", err)
			}

			// Decode
			decoded, err := DecodeMessage(bytes.NewReader(encoded))
			if err != nil {
				t.Fatalf("DecodeMessage() error = %v", err)
			}

			// Check header value
			val, ok := decoded.GetHeader(tt.header.Name)
			if !ok {
				t.Fatalf("Header %s not found", tt.header.Name)
			}

			// Compare values (handle []byte specially)
			if tt.header.Type == HeaderTypeByteArray {
				origBytes := tt.header.Value.([]byte)
				decodedBytes := val.([]byte)
				if !bytes.Equal(origBytes, decodedBytes) {
					t.Errorf("Byte array mismatch: got %v, want %v", decodedBytes, origBytes)
				}
			} else {
				if val != tt.header.Value {
					t.Errorf("Value mismatch: got %v (%T), want %v (%T)", val, val, tt.header.Value, tt.header.Value)
				}
			}
		})
	}
}

func TestCRCValidation(t *testing.T) {
	msg := CreateMessage(MessageTypeApplicationMessage, MessageFlagNone, []byte("test"))

	// Encode
	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("EncodeMessage() error = %v", err)
	}

	// Test corrupted prelude CRC
	t.Run("corrupted prelude CRC", func(t *testing.T) {
		corrupted := make([]byte, len(encoded))
		copy(corrupted, encoded)
		// Corrupt prelude CRC (bytes 8-11)
		corrupted[8] ^= 0xFF

		_, err := DecodeMessage(bytes.NewReader(corrupted))
		if err == nil {
			t.Error("Expected error for corrupted prelude CRC")
		}
	})

	// Test corrupted message CRC
	t.Run("corrupted message CRC", func(t *testing.T) {
		corrupted := make([]byte, len(encoded))
		copy(corrupted, encoded)
		// Corrupt message CRC (last 4 bytes)
		corrupted[len(corrupted)-1] ^= 0xFF

		_, err := DecodeMessage(bytes.NewReader(corrupted))
		if err == nil {
			t.Error("Expected error for corrupted message CRC")
		}
	})

	// Test corrupted payload
	t.Run("corrupted payload", func(t *testing.T) {
		corrupted := make([]byte, len(encoded))
		copy(corrupted, encoded)
		// Corrupt payload (somewhere in the middle)
		mid := len(corrupted) / 2
		corrupted[mid] ^= 0xFF

		_, err := DecodeMessage(bytes.NewReader(corrupted))
		if err == nil {
			t.Error("Expected error for corrupted payload")
		}
	})
}

func TestCreateMessage(t *testing.T) {
	msg := CreateMessage(MessageTypeApplicationMessage, MessageFlagTerminateStream, []byte("payload"))

	if msg.Type != MessageTypeApplicationMessage {
		t.Errorf("Type mismatch: got %v, want %v", msg.Type, MessageTypeApplicationMessage)
	}
	if msg.Flags != MessageFlagTerminateStream {
		t.Errorf("Flags mismatch: got %v, want %v", msg.Flags, MessageFlagTerminateStream)
	}
	if !bytes.Equal(msg.Payload, []byte("payload")) {
		t.Errorf("Payload mismatch: got %v, want %v", msg.Payload, []byte("payload"))
	}

	// Check that :message-type and :message-flags headers are set
	msgType, ok := msg.GetHeader(":message-type")
	if !ok {
		t.Error(":message-type header not found")
	}
	if msgType != int32(MessageTypeApplicationMessage) {
		t.Errorf(":message-type mismatch: got %v, want %v", msgType, int32(MessageTypeApplicationMessage))
	}

	msgFlags, ok := msg.GetHeader(":message-flags")
	if !ok {
		t.Error(":message-flags header not found")
	}
	if msgFlags != int32(MessageFlagTerminateStream) {
		t.Errorf(":message-flags mismatch: got %v, want %v", msgFlags, int32(MessageFlagTerminateStream))
	}
}

func TestMessageFlags(t *testing.T) {
	t.Run("HasFlag", func(t *testing.T) {
		flags := MessageFlagConnectionAccepted | MessageFlagTerminateStream

		if !flags.HasFlag(MessageFlagConnectionAccepted) {
			t.Error("Expected ConnectionAccepted flag to be set")
		}
		if !flags.HasFlag(MessageFlagTerminateStream) {
			t.Error("Expected TerminateStream flag to be set")
		}

		flagsNone := MessageFlagNone
		if flagsNone.HasFlag(MessageFlagConnectionAccepted) {
			t.Error("Expected no flags to be set")
		}
	})
}

func TestMessageGetters(t *testing.T) {
	msg := &Message{
		Headers: []Header{
			{Name: "string-header", Type: HeaderTypeString, Value: "test-value"},
			{Name: "int-header", Type: HeaderTypeInt32, Value: int32(42)},
		},
	}

	t.Run("GetStringHeader success", func(t *testing.T) {
		val, ok := msg.GetStringHeader("string-header")
		if !ok {
			t.Error("Expected to find string-header")
		}
		if val != "test-value" {
			t.Errorf("Got %v, want test-value", val)
		}
	})

	t.Run("GetStringHeader not found", func(t *testing.T) {
		_, ok := msg.GetStringHeader("missing-header")
		if ok {
			t.Error("Expected not to find missing-header")
		}
	})

	t.Run("GetStringHeader wrong type", func(t *testing.T) {
		_, ok := msg.GetStringHeader("int-header")
		if ok {
			t.Error("Expected type mismatch for int-header")
		}
	})
}

func TestSetHeader(t *testing.T) {
	msg := &Message{}

	// Set new header
	msg.SetHeader("test", HeaderTypeString, "value1")
	val, ok := msg.GetStringHeader("test")
	if !ok || val != "value1" {
		t.Errorf("Expected test=value1, got %v, %v", val, ok)
	}

	// Update existing header
	msg.SetHeader("test", HeaderTypeString, "value2")
	val, ok = msg.GetStringHeader("test")
	if !ok || val != "value2" {
		t.Errorf("Expected test=value2, got %v, %v", val, ok)
	}

	// Should only have one "test" header
	count := 0
	for _, h := range msg.Headers {
		if h.Name == "test" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 test header, got %d", count)
	}
}

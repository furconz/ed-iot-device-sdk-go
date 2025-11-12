package greengrassipc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/furconz/ed-iot-device-sdk-go/internal/eventstream"
	"github.com/furconz/ed-iot-device-sdk-go/internal/logging"
)

// Subscription represents a streaming IPC subscription
//
// Subscriptions receive messages asynchronously via channels.
// Use the Messages(), Errors(), and Done() methods to receive events.
// Always call Close() when done to clean up resources.
//
// Subscriptions automatically resubscribe on connection failure if reconnection is enabled.
type Subscription[T any] struct {
	stream    *eventstream.Stream
	messages  chan T
	errors    chan error
	done      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	client    *Client
	operation string
	request   interface{}
}

// Messages returns a channel that receives subscription messages
func (s *Subscription[T]) Messages() <-chan T {
	return s.messages
}

// Errors returns a channel that receives subscription errors
func (s *Subscription[T]) Errors() <-chan error {
	return s.errors
}

// Done returns a channel that is closed when the subscription ends
func (s *Subscription[T]) Done() <-chan struct{} {
	return s.done
}

// Close closes the subscription and releases resources
func (s *Subscription[T]) Close() error {
	s.cancel()
	return s.stream.Close()
}

// newSubscription creates a new subscription
func newSubscription[T any](ctx context.Context, client *Client, operation string, request interface{}, stream *eventstream.Stream) *Subscription[T] {
	subCtx, cancel := context.WithCancel(ctx)
	sub := &Subscription[T]{
		stream:    stream,
		messages:  make(chan T, 10),
		errors:    make(chan error, 1),
		done:      make(chan struct{}),
		ctx:       subCtx,
		cancel:    cancel,
		client:    client,
		operation: operation,
		request:   request,
	}

	// Start message processing goroutine
	go sub.processMessages()

	return sub
}

// processMessages processes incoming messages from the stream
func (s *Subscription[T]) processMessages() {
	defer close(s.done)
	defer close(s.messages)

	for {
		select {
		case <-s.ctx.Done():
			logging.Debug("Subscription context cancelled")
			return

		case err := <-s.stream.Errors():
			if err == nil {
				// Channel closed, stream ended
				logging.Debug("Subscription stream errors channel closed")
				return
			}

			// Check if it's a connection error that should trigger resubscribe
			if eventstream.IsConnectionError(err) {
				logging.Info("Subscription detected connection error, attempting to resubscribe: %v", err)

				// Attempt to resubscribe
				if s.resubscribe() {
					logging.Info("Subscription resubscribed successfully, resuming message processing")
					continue
				}

				// Resubscribe failed, report error and exit
				logging.Error("Subscription resubscribe failed")
				select {
				case s.errors <- err:
				case <-s.ctx.Done():
				}
				return
			}

			// Not a connection error, report it
			select {
			case s.errors <- err:
			case <-s.ctx.Done():
				return
			}

		case <-s.stream.Done():
			logging.Debug("Subscription stream done")
			return

		case msg := <-s.stream.Messages():
			if msg == nil {
				logging.Info("Subscription received nil message from stream")
				return
			}

			logging.Debug("Subscription processing message (payloadLen=%d)", len(msg.Payload))

			// Deserialize the message payload
			var event T
			if err := json.Unmarshal(msg.Payload, &event); err != nil {
				logging.Error("Subscription unmarshal error: %v", err)
				logging.Error("  Payload: %s", string(msg.Payload))
				select {
				case s.errors <- fmt.Errorf("failed to unmarshal message: %w", err):
				case <-s.ctx.Done():
					return
				}
				continue
			}

			logging.Debug("Subscription successfully deserialized message")

			// Send the event to the messages channel
			logging.Debug("Subscription sending message to channel...")
			select {
			case s.messages <- event:
				logging.Debug("Subscription message sent to channel successfully")
			case <-s.ctx.Done():
				logging.Debug("Subscription context cancelled while sending")
				return
			}
		}
	}
}

// resubscribe attempts to resubscribe after a connection failure
func (s *Subscription[T]) resubscribe() bool {
	maxAttempts := 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-s.ctx.Done():
			return false
		default:
		}

		logging.Info("Resubscribe attempt %d/%d for operation %s", attempt, maxAttempts, s.operation)

		// Create new stream
		stream := s.client.conn.NewStream(s.operation)

		// Attempt to activate with original request
		err := stream.Activate(s.ctx, s.request)
		if err == nil {
			// Success! Replace the old stream
			s.stream.Close() // Close old stream
			s.stream = stream
			logging.Info("Successfully resubscribed on attempt %d", attempt)
			return true
		}

		logging.Error("Resubscribe attempt %d failed: %v", attempt, err)

		// Don't retry on non-connection errors
		if !eventstream.IsConnectionError(err) {
			return false
		}

		// Wait before retrying (unless it's the last attempt)
		if attempt < maxAttempts {
			select {
			case <-s.ctx.Done():
				return false
			case <-time.After(time.Second * time.Duration(attempt)):
				// Exponential backoff: 1s, 2s, 3s
			}
		}
	}

	return false
}

// SubscribeToIoTCore subscribes to messages from AWS IoT Core
//
// Creates a subscription to receive messages published to the specified
// IoT Core topic. Messages are received asynchronously via the subscription's
// Messages() channel.
//
// Example:
//
//	sub, err := client.SubscribeToIoTCore(ctx, &SubscribeToIoTCoreRequest{
//	    TopicName: "my/topic",
//	    QOS: QOSAtLeastOnce,
//	})
//	if err != nil {
//	    return err
//	}
//	defer sub.Close()
//
//	for {
//	    select {
//	    case msg := <-sub.Messages():
//	        fmt.Printf("Received: %s\n", msg.Message.Payload)
//	    case err := <-sub.Errors():
//	        fmt.Printf("Error: %v\n", err)
//	    case <-sub.Done():
//	        return
//	    }
//	}
func (c *Client) SubscribeToIoTCore(ctx context.Context, req *SubscribeToIoTCoreRequest) (*Subscription[IoTCoreMessage], error) {
	stream := c.conn.NewStream(opSubscribeToIoTCore)
	if err := stream.Activate(ctx, req); err != nil {
		return nil, fmt.Errorf("failed to activate subscription: %w", err)
	}

	return newSubscription[IoTCoreMessage](ctx, c, opSubscribeToIoTCore, req, stream), nil
}

// SubscribeToTopic subscribes to messages from a local Greengrass topic
//
// Creates a subscription to receive messages published to local topics.
// This enables inter-component communication without going through IoT Core.
//
// Example:
//
//	sub, err := client.SubscribeToTopic(ctx, &SubscribeToTopicRequest{
//	    Topic: "local/topic",
//	    ReceiveMode: ReceiveModeReceiveAllMessages,
//	})
//	if err != nil {
//	    return err
//	}
//	defer sub.Close()
//
//	for msg := range sub.Messages() {
//	    if msg.BinaryMessage != nil {
//	        fmt.Printf("Binary: %s\n", msg.BinaryMessage.Message)
//	    }
//	}
func (c *Client) SubscribeToTopic(ctx context.Context, req *SubscribeToTopicRequest) (*Subscription[SubscriptionResponseMessage], error) {
	stream := c.conn.NewStream(opSubscribeToTopic)
	if err := stream.Activate(ctx, req); err != nil {
		return nil, fmt.Errorf("failed to activate subscription: %w", err)
	}

	return newSubscription[SubscriptionResponseMessage](ctx, c, opSubscribeToTopic, req, stream), nil
}

// SubscribeToComponentUpdates subscribes to component update notifications
//
// Receives notifications before and after component updates, allowing
// components to prepare for updates or perform cleanup afterwards.
func (c *Client) SubscribeToComponentUpdates(ctx context.Context, req *SubscribeToComponentUpdatesRequest) (*Subscription[ComponentUpdatePolicyEvents], error) {
	stream := c.conn.NewStream(opSubscribeToComponentUpdates)
	if err := stream.Activate(ctx, req); err != nil {
		return nil, fmt.Errorf("failed to activate subscription: %w", err)
	}

	return newSubscription[ComponentUpdatePolicyEvents](ctx, c, opSubscribeToComponentUpdates, req, stream), nil
}

// SubscribeToConfigurationUpdate subscribes to configuration changes
//
// Receives notifications when the component's configuration is updated.
func (c *Client) SubscribeToConfigurationUpdate(ctx context.Context, req *SubscribeToConfigurationUpdateRequest) (*Subscription[ConfigurationUpdateEvents], error) {
	stream := c.conn.NewStream(opSubscribeToConfigurationUpdate)
	if err := stream.Activate(ctx, req); err != nil {
		return nil, fmt.Errorf("failed to activate subscription: %w", err)
	}

	return newSubscription[ConfigurationUpdateEvents](ctx, c, opSubscribeToConfigurationUpdate, req, stream), nil
}

// SubscribeToValidateConfigurationUpdates subscribes to configuration validation requests
//
// Receives requests to validate proposed configuration changes before they
// are applied. The component should validate and respond using
// SendConfigurationValidityReport.
func (c *Client) SubscribeToValidateConfigurationUpdates(ctx context.Context, req *SubscribeToValidateConfigurationUpdatesRequest) (*Subscription[ValidateConfigurationUpdateEvents], error) {
	stream := c.conn.NewStream(opSubscribeToValidateConfigurationUpdates)
	if err := stream.Activate(ctx, req); err != nil {
		return nil, fmt.Errorf("failed to activate subscription: %w", err)
	}

	return newSubscription[ValidateConfigurationUpdateEvents](ctx, c, opSubscribeToValidateConfigurationUpdates, req, stream), nil
}

// SubscribeToCertificateUpdates subscribes to certificate update notifications
//
// Receives notifications when certificates are rotated or updated.
func (c *Client) SubscribeToCertificateUpdates(ctx context.Context, req *SubscribeToCertificateUpdatesRequest) (*Subscription[CertificateUpdateEvent], error) {
	stream := c.conn.NewStream(opSubscribeToCertificateUpdates)
	if err := stream.Activate(ctx, req); err != nil {
		return nil, fmt.Errorf("failed to activate subscription: %w", err)
	}

	return newSubscription[CertificateUpdateEvent](ctx, c, opSubscribeToCertificateUpdates, req, stream), nil
}

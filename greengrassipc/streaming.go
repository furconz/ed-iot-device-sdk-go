package greengrassipc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/furconz/ed-iot-device-sdk-go/internal/eventstream"
	"github.com/furconz/ed-iot-device-sdk-go/internal/logging"
)

// generationGuard ensures a function runs at most once per connection generation,
// so subscriptions resubscribe exactly once per reconnect even if multiple
// connection-error events arrive for the same drop.
type generationGuard struct {
	mu   sync.Mutex
	gen  uint64
	done bool
	ok   bool
}

// once runs fn at most once per generation and returns fn's (cached) result, so
// callers keep resubscribe's continue-vs-return control flow. Only one
// processMessages goroutine drives a given subscription's guard, so holding the
// lock across fn (resubscribe, up to ~30s) is uncontended.
func (g *generationGuard) once(gen uint64, fn func() bool) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.done && g.gen == gen {
		return g.ok
	}
	g.gen = gen
	g.done = true
	g.ok = fn()
	return g.ok
}

// Subscription represents a streaming IPC subscription
//
// Subscriptions receive messages asynchronously via channels.
// Use the Messages(), Errors(), and Done() methods to receive events.
// Always call Close() when done to clean up resources.
//
// Subscriptions automatically resubscribe on connection failure if reconnection is enabled.
type Subscription[T any] struct {
	stream     *eventstream.Stream
	messages   chan T
	errors     chan error
	done       chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	client     *Client
	operation  string
	request    interface{}
	label      string          // caller-supplied label for log disambiguation (may be empty)
	topic      string          // MQTT topic name captured at creation time (SubscribeToIoTCore only)
	resubGuard generationGuard // ensures resubscription fires exactly once per connection generation
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

// newSubscription creates a new subscription with no label.
// Delegates to newSubscriptionWithLabel with an empty label.
func newSubscription[T any](ctx context.Context, client *Client, operation string, request interface{}, stream *eventstream.Stream) *Subscription[T] {
	return newSubscriptionWithLabel[T](ctx, client, operation, request, stream, "")
}

// newSubscriptionWithLabel creates a new subscription with an optional caller-supplied label.
// The label is included in resubscribe log lines to distinguish subscriptions that share
// the same operation name (e.g. two aws.greengrass#SubscribeToIoTCore subscriptions on
// different topics). For IoTCore subscriptions the topic is extracted from req.TopicName
// when the request is a *SubscribeToIoTCoreRequest; for all other subscription types the
// topic field is left empty.
func newSubscriptionWithLabel[T any](ctx context.Context, client *Client, operation string, request interface{}, stream *eventstream.Stream, label string) *Subscription[T] {
	subCtx, cancel := context.WithCancel(ctx)

	topic := ""
	if iotReq, ok := request.(*SubscribeToIoTCoreRequest); ok {
		topic = iotReq.TopicName
	}

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
		label:     label,
		topic:     topic,
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

				// Attempt to resubscribe — guard ensures at most one resubscription attempt
				// per connection generation, even if multiple error events arrive for the
				// same reconnect. Cached result (ok/fail) is returned on subsequent calls
				// with the same generation, preserving the continue-vs-return control flow.
				if s.resubGuard.once(s.client.conn.Generation(), s.resubscribe) {
					logging.Info("Subscription resubscribed successfully, resuming message processing")
					continue
				}

				// Resubscribe definitively failed — report error and exit
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

		logging.Info("Resubscribe attempt %d/%d for operation %s (label=%q topic=%q stream=%d) - waiting for connection...",
			attempt, maxAttempts, s.operation, s.label, s.topic, s.stream.ID())

		// Wait for connection to be ready before attempting to resubscribe
		// Use a timeout to avoid infinite wait
		waitCtx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
		err := s.client.conn.WaitUntilReady(waitCtx)
		cancel()

		if err != nil {
			logging.Error("Resubscribe attempt %d (label=%q topic=%q stream=%d): connection not ready: %v",
				attempt, s.label, s.topic, s.stream.ID(), err)
			if attempt < maxAttempts {
				time.Sleep(time.Second * time.Duration(attempt))
			}
			continue
		}

		logging.Info("Connection ready, creating new stream for resubscribe attempt %d (label=%q topic=%q stream=%d)",
			attempt, s.label, s.topic, s.stream.ID())

		// NOW it's safe to create stream
		stream := s.client.conn.NewStream(s.operation)

		// Attempt to activate with original request
		err = stream.Activate(s.ctx, s.request)
		if err == nil {
			// Success! Replace the old stream
			s.stream.Close() // Close old stream
			s.stream = stream
			// D1a tripwire: assert this subscription is the registered owner of its id.
			// With P0 this always holds; a mismatch flags a residual collision.
			if owner, ok := s.client.conn.StreamOwner(s.stream.ID()); ok && owner != s.stream {
				logging.Error("D1a: resubscribed stream not registered as owner (label=%q topic=%q stream=%d)", s.label, s.topic, s.stream.ID())
			}
			logging.Info("Successfully resubscribed on attempt %d (label=%q topic=%q newStream=%d)",
				attempt, s.label, s.topic, s.stream.ID())
			return true
		}

		logging.Error("Resubscribe attempt %d failed (label=%q topic=%q): %v",
			attempt, s.label, s.topic, err)

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
	return c.SubscribeToIoTCoreWithLabel(ctx, req, "")
}

// SubscribeToIoTCoreWithLabel subscribes to messages from AWS IoT Core with a caller-supplied
// label that appears in reconnect/resubscribe log lines.
//
// When two subscriptions share the same operation name (aws.greengrass#SubscribeToIoTCore),
// the label lets operators identify which subscription is resubscribing without ambiguity.
// Pass an empty string to get the same behaviour as SubscribeToIoTCore.
func (c *Client) SubscribeToIoTCoreWithLabel(ctx context.Context, req *SubscribeToIoTCoreRequest, label string) (*Subscription[IoTCoreMessage], error) {
	stream := c.conn.NewStream(opSubscribeToIoTCore)
	if err := stream.Activate(ctx, req); err != nil {
		return nil, fmt.Errorf("failed to activate subscription: %w", err)
	}

	return newSubscriptionWithLabel[IoTCoreMessage](ctx, c, opSubscribeToIoTCore, req, stream, label), nil
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

package greengrassipc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/furconz/ed-iot-device-sdk-go/internal/eventstream"
)

// Subscription represents a streaming IPC subscription
//
// Subscriptions receive messages asynchronously via channels.
// Use the Messages(), Errors(), and Done() methods to receive events.
// Always call Close() when done to clean up resources.
type Subscription[T any] struct {
	stream   *eventstream.Stream
	messages chan T
	errors   chan error
	done     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
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
func newSubscription[T any](ctx context.Context, stream *eventstream.Stream) *Subscription[T] {
	subCtx, cancel := context.WithCancel(ctx)
	sub := &Subscription[T]{
		stream:   stream,
		messages: make(chan T, 10),
		errors:   make(chan error, 1),
		done:     make(chan struct{}),
		ctx:      subCtx,
		cancel:   cancel,
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
			return

		case err := <-s.stream.Errors():
			select {
			case s.errors <- err:
			case <-s.ctx.Done():
				return
			}

		case <-s.stream.Done():
			return

		case msg := <-s.stream.Messages():
			if msg == nil {
				return
			}

			// Deserialize the message payload
			var event T
			if err := json.Unmarshal(msg.Payload, &event); err != nil {
				select {
				case s.errors <- fmt.Errorf("failed to unmarshal message: %w", err):
				case <-s.ctx.Done():
					return
				}
				continue
			}

			// Send the event to the messages channel
			select {
			case s.messages <- event:
			case <-s.ctx.Done():
				return
			}
		}
	}
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

	return newSubscription[IoTCoreMessage](ctx, stream), nil
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

	return newSubscription[SubscriptionResponseMessage](ctx, stream), nil
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

	return newSubscription[ComponentUpdatePolicyEvents](ctx, stream), nil
}

// SubscribeToConfigurationUpdate subscribes to configuration changes
//
// Receives notifications when the component's configuration is updated.
func (c *Client) SubscribeToConfigurationUpdate(ctx context.Context, req *SubscribeToConfigurationUpdateRequest) (*Subscription[ConfigurationUpdateEvents], error) {
	stream := c.conn.NewStream(opSubscribeToConfigurationUpdate)
	if err := stream.Activate(ctx, req); err != nil {
		return nil, fmt.Errorf("failed to activate subscription: %w", err)
	}

	return newSubscription[ConfigurationUpdateEvents](ctx, stream), nil
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

	return newSubscription[ValidateConfigurationUpdateEvents](ctx, stream), nil
}

// SubscribeToCertificateUpdates subscribes to certificate update notifications
//
// Receives notifications when certificates are rotated or updated.
func (c *Client) SubscribeToCertificateUpdates(ctx context.Context, req *SubscribeToCertificateUpdatesRequest) (*Subscription[CertificateUpdateEvent], error) {
	stream := c.conn.NewStream(opSubscribeToCertificateUpdates)
	if err := stream.Activate(ctx, req); err != nil {
		return nil, fmt.Errorf("failed to activate subscription: %w", err)
	}

	return newSubscription[CertificateUpdateEvent](ctx, stream), nil
}

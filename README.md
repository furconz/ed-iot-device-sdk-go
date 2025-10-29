# AWS IoT Device SDK for Go v2

A Go implementation of the AWS IoT Device SDK v2, focused on AWS IoT Greengrass v2 components.

> **Note**: This is an unofficial, community-maintained derivative work based on the official [AWS IoT Device SDK for Python v2](https://github.com/aws/aws-iot-device-sdk-python-v2) and [AWS IoT Device SDK for JavaScript v2](https://github.com/aws/aws-iot-device-sdk-js-v2). It implements the same protocols and APIs adapted for idiomatic Go usage.

## Overview

This SDK provides a Go interface for AWS IoT Greengrass IPC (Inter-Process Communication), enabling Greengrass v2 components to:
- Publish and subscribe to AWS IoT Core MQTT topics
- Communicate with other local components via pub/sub
- Manage component lifecycle and configuration
- Access AWS IoT Device Shadows
- Retrieve secrets from AWS Secrets Manager
- And more...

## Features

- **Greengrass IPC**: Full support for all 30+ Greengrass IPC operations
- **Context Support**: All operations accept `context.Context` for cancellation and timeouts
- **Type Safety**: Strongly-typed requests, responses, and enums
- **Idiomatic Go**: Channel-based streaming, standard error handling
- **Pure Go**: No CGo dependencies, works on all platforms

See [FEATURES.md](FEATURES.md) for a complete feature matrix.

## Installation

```bash
go get github.com/furconz/ed-iot-device-sdk-go
```

## Examples

See the [examples/](examples/) directory for complete working examples:

- **[echo-component](examples/echo-component/)** - A full-featured Greengrass component that demonstrates:
  - Subscribing to and publishing to IoT Core topics
  - Configuration management with dynamic reload
  - Custom metrics reporting to CloudWatch
  - Graceful shutdown handling
  - Component lifecycle management

## Quick Start

### Basic IPC Client

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/furconz/ed-iot-device-sdk-go/greengrassipc"
)

func main() {
	ctx := context.Background()

	// Create IPC client (auto-detects configuration from environment)
	client, err := greengrassipc.NewClient(ctx, nil)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Get component configuration
	resp, err := client.GetConfiguration(ctx, &greengrassipc.GetConfigurationRequest{})
	if err != nil {
		log.Fatalf("Failed to get configuration: %v", err)
	}

	fmt.Printf("Configuration: %+v\n", resp.Value)
}
```

### Publishing to IoT Core

```go
// Publish a message to AWS IoT Core
_, err := client.PublishToIoTCore(ctx, &greengrassipc.PublishToIoTCoreRequest{
	TopicName: "my/topic",
	QOS:       greengrassipc.QOSAtLeastOnce,
	Payload:   []byte("Hello from Greengrass!"),
})
if err != nil {
	log.Fatalf("Failed to publish: %v", err)
}
```

### Subscribing to IoT Core Topics

```go
// Subscribe to IoT Core messages
sub, err := client.SubscribeToIoTCore(ctx, &greengrassipc.SubscribeToIoTCoreRequest{
	TopicName: "my/topic",
	QOS:       greengrassipc.QOSAtLeastOnce,
})
if err != nil {
	log.Fatalf("Failed to subscribe: %v", err)
}
defer sub.Close()

// Receive messages
for {
	select {
	case msg := <-sub.Messages():
		fmt.Printf("Received: %s\n", msg.Message.Payload)

	case err := <-sub.Errors():
		log.Printf("Subscription error: %v\n", err)

	case <-sub.Done():
		log.Println("Subscription closed")
		return

	case <-ctx.Done():
		return
	}
}
```

### Local Pub/Sub

```go
// Publish to local Greengrass topic
_, err := client.PublishToTopic(ctx, &greengrassipc.PublishToTopicRequest{
	Topic: "local/topic",
	PublishMessage: &greengrassipc.PublishMessage{
		BinaryMessage: &greengrassipc.BinaryMessage{
			Message: []byte("Hello other components!"),
		},
	},
})

// Subscribe to local topic
sub, err := client.SubscribeToTopic(ctx, &greengrassipc.SubscribeToTopicRequest{
	Topic:       "local/topic",
	ReceiveMode: greengrassipc.ReceiveModeReceiveAllMessages,
})
defer sub.Close()

for msg := range sub.Messages() {
	if msg.BinaryMessage != nil {
		fmt.Printf("Received: %s\n", msg.BinaryMessage.Message)
	}
}
```

### Working with Shadows

```go
// Update shadow
shadowState := map[string]interface{}{
	"state": map[string]interface{}{
		"reported": map[string]interface{}{
			"temperature": 72.5,
			"humidity":    45,
		},
	},
}
payload, _ := json.Marshal(shadowState)

_, err := client.UpdateThingShadow(ctx, &greengrassipc.UpdateThingShadowRequest{
	ThingName: "MyThing",
	Payload:   payload,
})

// Get shadow
resp, err := client.GetThingShadow(ctx, &greengrassipc.GetThingShadowRequest{
	ThingName: "MyThing",
})
if err != nil {
	log.Fatal(err)
}

// Parse shadow document
doc, err := greengrassipc.ParseShadowDocument(resp.Payload)
if err != nil {
	log.Fatal(err)
}
fmt.Printf("Shadow state: %+v\n", doc.State)
```

### Accessing Secrets

```go
resp, err := client.GetSecretValue(ctx, &greengrassipc.GetSecretValueRequest{
	SecretID: "my-secret",
})
if err != nil {
	log.Fatal(err)
}

if resp.SecretValue.SecretString != nil {
	fmt.Printf("Secret: %s\n", *resp.SecretValue.SecretString)
}
```

### Component Lifecycle

```go
// Report component state
err := client.UpdateState(ctx, &greengrassipc.UpdateStateRequest{
	State: greengrassipc.ReportedLifecycleStateRunning,
})

// Subscribe to component updates
sub, err := client.SubscribeToComponentUpdates(ctx,
	&greengrassipc.SubscribeToComponentUpdatesRequest{})
defer sub.Close()

for event := range sub.Messages() {
	if event.PreUpdateEvent != nil {
		fmt.Printf("Pre-update for deployment: %s\n",
			event.PreUpdateEvent.DeploymentID)
		// Perform pre-update tasks...
	}
	if event.PostUpdateEvent != nil {
		fmt.Printf("Post-update for deployment: %s\n",
			event.PostUpdateEvent.DeploymentID)
		// Perform post-update tasks...
	}
}
```

### Configuration Management

```go
// Get configuration
resp, err := client.GetConfiguration(ctx, &greengrassipc.GetConfigurationRequest{})
if err != nil {
	log.Fatal(err)
}
fmt.Printf("Config: %+v\n", resp.Value)

// Subscribe to configuration updates
sub, err := client.SubscribeToConfigurationUpdate(ctx,
	&greengrassipc.SubscribeToConfigurationUpdateRequest{})
defer sub.Close()

for event := range sub.Messages() {
	if event.ConfigurationUpdateEvent != nil {
		fmt.Println("Configuration updated!")
		// Reload configuration...
	}
}
```

## Environment Variables

The client auto-detects configuration from these environment variables (automatically set by Greengrass):

- `AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT` - Path to the IPC socket
- `SVCUID` - Authentication token for IPC

You can also provide these explicitly via `ClientConfig`:

```go
client, err := greengrassipc.NewClient(ctx, &greengrassipc.ClientConfig{
	SocketPath: "/greengrass/v2/ipc.socket",
	AuthToken:  "my-auth-token",
})
```

## API Documentation

Full API documentation is available at: [pkg.go.dev](https://pkg.go.dev/github.com/furconz/ed-iot-device-sdk-go/greengrassipc)

Or generate locally:

```bash
go doc -all github.com/furconz/ed-iot-device-sdk-go/greengrassipc
```

## Project Structure

```
awsiotdevicesdk/
├── greengrassipc/          # Greengrass IPC client
│   ├── client.go           # Main client and connection management
│   ├── streaming.go        # Streaming subscriptions
│   ├── operations_*.go     # IPC operations by category
│   ├── types.go            # Common types and enums
│   └── types_*.go          # Request/response types by category
├── internal/
│   └── eventstream/        # EventStream RPC protocol
│       ├── framing.go      # Message encoding/decoding
│       ├── connection.go   # Connection management
│       └── types.go        # Protocol types
├── PLAN.md                 # Implementation plan and progress
├── FEATURES.md             # Feature matrix
└── README.md               # This file
```

## Design Principles

1. **Context-First**: All operations accept `context.Context` for cancellation and timeouts
2. **Type Safety**: Strongly-typed requests, responses, and enums - no dynamic maps
3. **Idiomatic Go**: Channel-based streams, standard error handling, goroutine-safe
4. **Minimal Dependencies**: Only standard library for runtime
5. **Logical Organization**: Files grouped by functional area for easy navigation

## Testing

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run specific package tests
go test ./internal/eventstream/...
```

## Contributing

Contributions are welcome! Please ensure:
- All code is properly documented with godoc comments
- Tests are included for new functionality
- Code follows Go conventions and passes `go vet`
- Files are logically organized (use appropriate prefixes)

## Known Limitations

- **Examples**: Working code examples are planned but not yet implemented (see FEATURES.md)
- **Standalone MQTT**: Direct MQTT client (without IPC) is planned for future
- **Integration Tests**: Require a Greengrass environment to run

See [PLAN.md](PLAN.md) for implementation progress and [FEATURES.md](FEATURES.md) for detailed feature status.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

This is a derivative work based on the official AWS IoT Device SDKs:
- [AWS IoT Device SDK for Python v2](https://github.com/aws/aws-iot-device-sdk-python-v2) (Apache 2.0)
- [AWS IoT Device SDK for JavaScript v2](https://github.com/aws/aws-iot-device-sdk-js-v2) (Apache 2.0)

This Go implementation follows the same protocols, API patterns, and design principles as the official SDKs, adapted for idiomatic Go usage. We gratefully acknowledge AWS for their excellent work on the original SDKs and the EventStream RPC protocol.

## Support

- **Issues**: [GitHub Issues](https://github.com/furconz/ed-iot-device-sdk-go/issues)
- **Documentation**: [AWS IoT Greengrass v2 Documentation](https://docs.aws.amazon.com/greengrass/v2/developerguide/)

## Related Projects

- [AWS IoT Device SDK for Python v2](https://github.com/aws/aws-iot-device-sdk-python-v2)
- [AWS IoT Device SDK for JavaScript v2](https://github.com/aws/aws-iot-device-sdk-js-v2)
- [AWS IoT Greengrass Documentation](https://docs.aws.amazon.com/greengrass/)

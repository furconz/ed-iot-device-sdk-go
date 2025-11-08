# IPC Logging

This package provides centralized logging for the Greengrass IPC SDK with three logging levels.

## Logging Levels

### Debug Messages (`logging.Debug()`)

Debug messages are gated behind the `IPC_DEBUG` environment variable. By default, debug messages are **hidden** to reduce log density.

To enable debug messages, set:
```bash
export IPC_DEBUG=on
```

Debug messages are prefixed with `[IPC DEBUG]` and include verbose, high-volume protocol details:
- Message encoding/decoding
- Wire-level protocol details
- Detailed stream lifecycle events
- Verbose subscription message processing

### Info Messages (`logging.Info()`)

Info messages are **always shown** regardless of the `IPC_DEBUG` setting. They are prefixed with `[IPC INFO]` and include important, low-volume lifecycle events:
- Connection establishment
- Configuration discovery (socket path, auth token from environment)
- Unexpected conditions (nil messages, closed stream receiving messages)

### Error Messages (`logging.Error()`)

Error messages are **always shown** regardless of the `IPC_DEBUG` setting. They are prefixed with `[IPC ERROR]` and include critical errors:
- Decode/unmarshal failures
- Message loss (channel full, dropping messages)
- Handshake failures
- Connection-level errors
- Stream-level application errors
- Request-response operation failures

## Performance

The debug gating is highly performant:
- The `IPC_DEBUG` environment variable is checked **once** at package initialization
- When debug is disabled, `logging.Debug()` returns immediately without any string formatting overhead
- No performance impact on production deployments with debug disabled

## Examples

### Running with Debug Disabled (Default)
```bash
# Debug messages hidden, only errors shown
./your-greengrass-component
```

### Running with Debug Enabled
```bash
# All debug messages shown
IPC_DEBUG=on ./your-greengrass-component
```

### Sample Output

**Debug Disabled (Default):**
```
[IPC INFO] Using socket path from env: /greengrass/v2/ipc.socket
[IPC INFO] Connecting to socket: /greengrass/v2/ipc.socket
[IPC INFO] Socket connected successfully
[IPC ERROR] Stream 2 message channel full, dropping message
[IPC ERROR] Subscription unmarshal error: json: cannot unmarshal string into Go value of type IoTCoreMessage
[IPC ERROR]   Payload: "invalid json"
```

**Debug Enabled (IPC_DEBUG=on):**
```
[IPC INFO] Using socket path from env: /greengrass/v2/ipc.socket
[IPC INFO] Connecting to socket: /greengrass/v2/ipc.socket
[IPC INFO] Socket connected successfully
[IPC DEBUG] Sending CONNECT message: type=CONNECT flags=0x0 headers=1 payloadLen=45
[IPC DEBUG] CONNECT sent, waiting for CONNACK...
[IPC DEBUG] Received message: type=CONNACK (1) flags=0x1 headers=0 payloadLen=0
[IPC DEBUG] Created stream 1 for operation: aws.greengrass#PublishToIoTCore
[IPC DEBUG] Activating stream 1:
[IPC DEBUG]   Operation: aws.greengrass#PublishToIoTCore
[IPC DEBUG]   Request payload: {"topicName":"test/topic","payload":"..."}
[IPC DEBUG] Stream 1 activated successfully
[IPC DEBUG] Raw message received: type=APPLICATION_MESSAGE flags=0x0 payloadLen=128 headerCount=2
[IPC ERROR] Stream 2 message channel full, dropping message
[IPC ERROR] Subscription unmarshal error: json: cannot unmarshal string into Go value of type IoTCoreMessage
[IPC ERROR]   Payload: "invalid json"
```

## Code Structure

- `Debug(format string, args ...interface{})` - Logs verbose debug messages when `IPC_DEBUG=on`
- `Info(format string, args ...interface{})` - Always logs important lifecycle events
- `Error(format string, args ...interface{})` - Always logs error messages
- All functions use `log.Printf` internally with appropriate prefixes

## Testing

Tests are included to verify:
1. Debug messages are hidden by default
2. Debug messages are shown when `IPC_DEBUG=on`
3. Info messages are always shown
4. Error messages are always shown
5. Performance characteristics when debug is disabled

Run tests with:
```bash
go test ./internal/logging/
```

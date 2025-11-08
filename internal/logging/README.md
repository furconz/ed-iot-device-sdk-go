# IPC Logging

This package provides centralized logging for the Greengrass IPC SDK with environment-variable-based debug gating.

## Usage

### Debug Messages

Debug messages are gated behind the `IPC_DEBUG` environment variable. By default, debug messages are **hidden** to reduce log density.

To enable debug messages, set:
```bash
export IPC_DEBUG=on
```

Debug messages are prefixed with `[IPC DEBUG]` and include detailed information about:
- Socket connections and handshakes
- Message encoding/decoding
- Stream lifecycle (creation, activation, termination)
- Subscription message processing
- Request/response operations

### Error Messages

Error messages are **always shown** regardless of the `IPC_DEBUG` setting. They are prefixed with `[IPC ERROR]` and include:
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

**Debug Disabled:**
```
[IPC ERROR] Stream 2 received ApplicationError: operation failed
[IPC ERROR]   Payload: {"errorMessage":"Invalid request"}
```

**Debug Enabled:**
```
[IPC DEBUG] Connecting to socket: /tmp/greengrass.sock
[IPC DEBUG] Socket connected successfully
[IPC DEBUG] Sending CONNECT message: type=CONNECT flags=0x0 headers=1 payloadLen=45
[IPC DEBUG] CONNECT sent, waiting for CONNACK...
[IPC DEBUG] Received message: type=CONNACK (1) flags=0x1 headers=0 payloadLen=0
[IPC DEBUG] Created stream 1 for operation: aws.greengrass#PublishToIoTCore
[IPC DEBUG] Activating stream 1:
[IPC DEBUG]   Operation: aws.greengrass#PublishToIoTCore
[IPC DEBUG]   Request payload: {"topicName":"test/topic","payload":"..."}
[IPC DEBUG] Stream 1 activated successfully
[IPC ERROR] Stream 2 received ApplicationError: operation failed
[IPC ERROR]   Payload: {"errorMessage":"Invalid request"}
```

## Code Structure

- `Debug(format string, args ...interface{})` - Logs debug messages when `IPC_DEBUG=on`
- `Error(format string, args ...interface{})` - Always logs error messages
- Both functions use `log.Printf` internally with appropriate prefixes

## Testing

Tests are included to verify:
1. Debug messages are hidden by default
2. Debug messages are shown when `IPC_DEBUG=on`
3. Error messages are always shown
4. Performance characteristics when debug is disabled

Run tests with:
```bash
go test ./internal/logging/
```

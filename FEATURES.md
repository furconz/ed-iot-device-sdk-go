# AWS IoT Device SDK for Go v2 - Feature Matrix

This document tracks which features from the official AWS IoT Device SDKs (Python, JavaScript) are implemented, planned (TODO), or explicitly not being implemented in the Go SDK.

## Legend
- ✅ **Implemented** - Feature is complete and tested
- ⏳ **In Progress** - Currently being implemented
- 📋 **TODO** - Planned for future implementation
- ❌ **Not Implementing** - Will not be implemented (with rationale)

---

## Core Connectivity

### MQTT Protocol Support

| Feature | Status | Notes |
|---------|--------|-------|
| MQTT 5.0 via IPC | ⏳ | Primary method for Greengrass components |
| MQTT 5.0 standalone client | 📋 | Future: Direct connection to IoT Core |
| MQTT 3.1.1 | ❌ | Not implementing - MQTT5 only |
| TLS 1.2 | 📋 | For standalone MQTT client |
| TLS 1.3 | 📋 | For standalone MQTT client |

### Authentication Methods

| Feature | Status | Notes |
|---------|--------|-------|
| Greengrass IPC auth token | ⏳ | Primary auth for components |
| X.509 mTLS certificates | 📋 | For standalone MQTT client |
| WebSocket with SigV4 | 📋 | AWS credentials-based auth |
| Custom authorizers | 📋 | Lambda-based custom auth |
| PKCS#11 (HSM) | ❌ | Complex, not needed initially |
| PKCS#12 | ❌ | Platform-specific, not needed |
| Windows certificate store | ❌ | Platform-specific, not needed |

---

## Greengrass IPC Operations

All Greengrass IPC operations use the EventStream RPC protocol over Unix domain sockets.

### IoT Core Integration (MQTT via IPC)

| Operation | Type | Status | Notes |
|-----------|------|--------|-------|
| `PublishToIoTCore` | Request-Response | ⏳ | Publish messages to IoT Core |
| `SubscribeToIoTCore` | Streaming | ⏳ | Subscribe to IoT Core topics |

### Local Pub/Sub

| Operation | Type | Status | Notes |
|-----------|------|--------|-------|
| `PublishToTopic` | Request-Response | ⏳ | Local inter-component messaging |
| `SubscribeToTopic` | Streaming | ⏳ | Subscribe to local topics |

### Component Lifecycle Management

| Operation | Type | Status | Notes |
|-----------|------|--------|-------|
| `GetComponentDetails` | Request-Response | ⏳ | Get component metadata |
| `ListComponents` | Request-Response | ⏳ | List all components |
| `RestartComponent` | Request-Response | ⏳ | Restart a component |
| `StopComponent` | Request-Response | ⏳ | Stop a component |
| `PauseComponent` | Request-Response | ⏳ | Pause a component |
| `ResumeComponent` | Request-Response | ⏳ | Resume a paused component |
| `DeferComponentUpdate` | Request-Response | ⏳ | Defer component updates |
| `UpdateState` | Request-Response | ⏳ | Update component state |
| `SubscribeToComponentUpdates` | Streaming | ⏳ | Monitor component updates |

### Configuration Management

| Operation | Type | Status | Notes |
|-----------|------|--------|-------|
| `GetConfiguration` | Request-Response | ⏳ | Get component configuration |
| `UpdateConfiguration` | Request-Response | ⏳ | Update configuration |
| `SendConfigurationValidityReport` | Request-Response | ⏳ | Report config validation result |
| `SubscribeToConfigurationUpdate` | Streaming | ⏳ | Monitor config changes |
| `SubscribeToValidateConfigurationUpdates` | Streaming | ⏳ | Validate config before applying |

### Deployment Management

| Operation | Type | Status | Notes |
|-----------|------|--------|-------|
| `CreateLocalDeployment` | Request-Response | ⏳ | Create a local deployment |
| `CancelLocalDeployment` | Request-Response | ⏳ | Cancel a deployment |
| `GetLocalDeploymentStatus` | Request-Response | ⏳ | Get deployment status |
| `ListLocalDeployments` | Request-Response | ⏳ | List all deployments |

### Device Shadow Operations

| Operation | Type | Status | Notes |
|-----------|------|--------|-------|
| `GetThingShadow` | Request-Response | ⏳ | Get shadow state |
| `UpdateThingShadow` | Request-Response | ⏳ | Update shadow state |
| `DeleteThingShadow` | Request-Response | ⏳ | Delete a shadow |
| `ListNamedShadowsForThing` | Request-Response | ⏳ | List all named shadows |

### Secrets Management

| Operation | Type | Status | Notes |
|-----------|------|--------|-------|
| `GetSecretValue` | Request-Response | ⏳ | Retrieve secret from Secrets Manager |

### Client Device Authentication

| Operation | Type | Status | Notes |
|-----------|------|--------|-------|
| `AuthorizeClientDeviceAction` | Request-Response | ⏳ | Authorize client device actions |
| `GetClientDeviceAuthToken` | Request-Response | ⏳ | Get auth token for client device |
| `ValidateAuthorizationToken` | Request-Response | ⏳ | Validate auth token |
| `VerifyClientDeviceIdentity` | Request-Response | ⏳ | Verify client device identity |

### Certificate Management

| Operation | Type | Status | Notes |
|-----------|------|--------|-------|
| `SubscribeToCertificateUpdates` | Streaming | ⏳ | Monitor certificate rotation |

### Monitoring & Debugging

| Operation | Type | Status | Notes |
|-----------|------|--------|-------|
| `PutComponentMetric` | Request-Response | ⏳ | Publish custom metrics |
| `CreateDebugPassword` | Request-Response | ⏳ | Create debug password |

---

## Service Clients (High-Level APIs)

### Device Shadow Service

| Feature | Status | Notes |
|---------|--------|-------|
| Get shadow (classic) | 📋 | Via IPC or MQTT |
| Update shadow (classic) | 📋 | Via IPC or MQTT |
| Delete shadow (classic) | 📋 | Via IPC or MQTT |
| Named shadows | 📋 | Multiple shadows per device |
| Shadow delta notifications | 📋 | Subscribe to desired/reported diffs |
| Type-safe shadow state | 📋 | Strongly-typed shadow structures |

### Jobs Service

| Feature | Status | Notes |
|---------|--------|-------|
| Get pending jobs | 📋 | Retrieve queued jobs |
| Start next job | 📋 | Begin job execution |
| Update job status | 📋 | Report progress/completion |
| Job notifications | 📋 | Subscribe to job updates |

### Fleet Provisioning

| Feature | Status | Notes |
|---------|--------|-------|
| Create keys and certificate | ❌ | Not needed for Greengrass |
| Create certificate from CSR | ❌ | Not needed for Greengrass |
| Register thing | ❌ | Not needed for Greengrass |

**Rationale:** Greengrass components use the core device's credentials. Fleet provisioning is only needed for standalone devices connecting directly to IoT Core.

### Greengrass Discovery

| Feature | Status | Notes |
|---------|--------|-------|
| Discover Greengrass cores | ❌ | Not needed for components |
| Get connectivity info | ❌ | Not needed for components |

**Rationale:** Greengrass components run on the core itself and don't need to discover it. This is only for client devices connecting to a Greengrass core.

---

## Protocol Implementations

### EventStream RPC

| Feature | Status | Notes |
|---------|--------|-------|
| Message framing | ⏳ | Wire protocol implementation |
| Connection handshake | ⏳ | CONNECT/CONNACK with auth |
| Request-response operations | ⏳ | Single request, single response |
| Streaming operations | ⏳ | Bidirectional streaming |
| Error handling | ⏳ | Typed errors from service |
| Header encoding/decoding | ⏳ | Protocol-level headers |
| JSON payload serialization | ⏳ | Request/response bodies |

### MQTT Request-Response Pattern

| Feature | Status | Notes |
|---------|--------|-------|
| Correlation tokens | 📋 | Match requests to responses |
| Publish-subscribe abstraction | 📋 | High-level API over pub/sub |
| Timeout handling | 📋 | Request timeout support |

---

## Go-Specific Features

### Idiomatic Go Patterns

| Feature | Status | Notes |
|---------|--------|-------|
| Context support | ⏳ | All APIs accept context.Context |
| Channel-based streaming | ⏳ | Idiomatic async message handling |
| Error wrapping | ⏳ | Standard Go error patterns |
| Typed constants for enums | ⏳ | Type-safe enum values |
| Builder pattern | ⏳ | Fluent client configuration |

### Concurrency & Safety

| Feature | Status | Notes |
|---------|--------|-------|
| Thread-safe client | ⏳ | Safe for concurrent use |
| Graceful shutdown | ⏳ | Clean resource cleanup |
| Context cancellation | ⏳ | Cancel operations in-flight |

---

## Documentation & Examples

| Feature | Status | Notes |
|---------|--------|-------|
| README with quick start | 📋 | Basic usage guide |
| API documentation (godoc) | ⏳ | Inline documentation |
| Greengrass IPC example | 📋 | Complete working example |
| MQTT pub/sub example | 📋 | Standalone MQTT usage |
| Shadow operations example | 📋 | Shadow update example |
| Jobs execution example | 📋 | Job processing example |

---

## Testing

| Feature | Status | Notes |
|---------|--------|-------|
| Unit tests | ⏳ | Core functionality tests |
| EventStream protocol tests | ⏳ | Compare to AWS implementation |
| Integration tests | 📋 | Requires Greengrass environment |
| Mock IPC server | 📋 | For testing without Greengrass |

---

## Platform Support

| Platform | Status | Notes |
|----------|--------|-------|
| Linux | ⏳ | Primary platform |
| Windows | ⏳ | Unix domain socket support added in Windows 10+ |
| macOS | ⏳ | Full support expected |
| ARM64 | ⏳ | Greengrass runs on ARM |
| x86_64 | ⏳ | Standard architecture |

---

## Summary Statistics

### Overall Progress
- **Total Features Planned:** 60+
- **Implemented:** 0 (0%)
- **In Progress:** 45 (75%)
- **TODO:** 12 (20%)
- **Not Implementing:** 8 (5%)

### By Category
- **Greengrass IPC Operations:** 0/30+ implemented
- **Service Clients:** 0/2 planned (Shadow, Jobs)
- **Protocol Implementations:** 0/1 in progress (EventStream RPC)
- **Documentation:** 0/3 planned
- **Examples:** 0/4 planned

---

**Last Updated:** 2025-10-30

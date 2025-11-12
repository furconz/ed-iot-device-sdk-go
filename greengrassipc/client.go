package greengrassipc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/furconz/ed-iot-device-sdk-go/internal/eventstream"
	"github.com/furconz/ed-iot-device-sdk-go/internal/logging"
)

// Client represents a Greengrass IPC client
type Client struct {
	conn *eventstream.Connection
}

// ClientConfig holds configuration for the IPC client
type ClientConfig struct {
	// SocketPath is the path to the Greengrass IPC socket
	// If empty, will be read from AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT env var
	SocketPath string

	// AuthToken is the authentication token for IPC
	// If empty, will be read from SVCUID env var
	AuthToken string

	// Reconnection holds reconnection configuration
	// If nil, default reconnection settings are used
	Reconnection *ReconnectionConfig
}

// ReconnectionConfig holds configuration for automatic reconnection
type ReconnectionConfig struct {
	// Enabled controls whether automatic reconnection is enabled
	// Default: true
	Enabled bool

	// PingInterval is how often to send keepalive pings
	// Default: 30 seconds
	PingInterval time.Duration

	// PingTimeout is how long to wait for ping response before considering connection stale
	// Default: 10 seconds
	PingTimeout time.Duration

	// MaxRetries is the maximum number of retry attempts for request-response operations
	// Default: 3
	MaxRetries int

	// OnDisconnected is called when connection is lost (before reconnection attempts)
	OnDisconnected func(err error)

	// OnReconnected is called when connection is successfully re-established
	OnReconnected func()
}

// NewClient creates a new Greengrass IPC client
//
// If config is nil or has empty values, the client will attempt to auto-detect
// configuration from environment variables:
// - AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT: Socket path
// - SVCUID: Authentication token
func NewClient(ctx context.Context, config *ClientConfig) (*Client, error) {
	cfg := ClientConfig{}
	if config != nil {
		cfg = *config
	}

	// Auto-detect socket path from environment if not provided
	if cfg.SocketPath == "" {
		cfg.SocketPath = os.Getenv("AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT")
		if cfg.SocketPath == "" {
			return nil, fmt.Errorf("socket path not provided and AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT not set")
		}
		logging.Info("Using socket path from env: %s", cfg.SocketPath)
	}

	// Auto-detect auth token from environment if not provided
	if cfg.AuthToken == "" {
		cfg.AuthToken = os.Getenv("SVCUID")
		if cfg.AuthToken == "" {
			return nil, fmt.Errorf("auth token not provided and SVCUID not set")
		}
		logging.Info("Using auth token from env (length=%d)", len(cfg.AuthToken))
	}

	// Apply default reconnection settings if not provided
	reconnectCfg := cfg.Reconnection
	if reconnectCfg == nil {
		reconnectCfg = &ReconnectionConfig{}
	}
	if reconnectCfg.PingInterval == 0 {
		reconnectCfg.PingInterval = 30 * time.Second
	}
	if reconnectCfg.PingTimeout == 0 {
		reconnectCfg.PingTimeout = 10 * time.Second
	}
	if reconnectCfg.MaxRetries == 0 {
		reconnectCfg.MaxRetries = 3
	}
	// Default to enabled unless explicitly disabled
	if cfg.Reconnection == nil {
		reconnectCfg.Enabled = true
	}

	// Connect to IPC
	conn, err := eventstream.Connect(ctx, eventstream.ConnectionConfig{
		SocketPath:         cfg.SocketPath,
		AuthToken:          cfg.AuthToken,
		EnableReconnection: reconnectCfg.Enabled,
		PingInterval:       reconnectCfg.PingInterval,
		PingTimeout:        reconnectCfg.PingTimeout,
		MaxRetries:         reconnectCfg.MaxRetries,
		OnDisconnected:     reconnectCfg.OnDisconnected,
		OnReconnected:      reconnectCfg.OnReconnected,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Greengrass IPC: %w", err)
	}

	return &Client{
		conn: conn,
	}, nil
}

// Close closes the IPC connection
func (c *Client) Close() error {
	return c.conn.Close()
}

// requestResponse performs a request-response operation
func (c *Client) requestResponse(ctx context.Context, operation string, request interface{}, response interface{}) error {
	payload, err := c.conn.RequestResponse(ctx, operation, request)
	if err != nil {
		return err
	}

	if response != nil && len(payload) > 0 {
		if err := json.Unmarshal(payload, response); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	}

	return nil
}

// Operation name constants
const (
	opPublishToIoTCore                          = "aws.greengrass#PublishToIoTCore"
	opSubscribeToIoTCore                        = "aws.greengrass#SubscribeToIoTCore"
	opPublishToTopic                            = "aws.greengrass#PublishToTopic"
	opSubscribeToTopic                          = "aws.greengrass#SubscribeToTopic"
	opGetComponentDetails                       = "aws.greengrass#GetComponentDetails"
	opListComponents                            = "aws.greengrass#ListComponents"
	opRestartComponent                          = "aws.greengrass#RestartComponent"
	opStopComponent                             = "aws.greengrass#StopComponent"
	opPauseComponent                            = "aws.greengrass#PauseComponent"
	opResumeComponent                           = "aws.greengrass#ResumeComponent"
	opUpdateState                               = "aws.greengrass#UpdateState"
	opDeferComponentUpdate                      = "aws.greengrass#DeferComponentUpdate"
	opSubscribeToComponentUpdates               = "aws.greengrass#SubscribeToComponentUpdates"
	opGetConfiguration                          = "aws.greengrass#GetConfiguration"
	opUpdateConfiguration                       = "aws.greengrass#UpdateConfiguration"
	opSendConfigurationValidityReport           = "aws.greengrass#SendConfigurationValidityReport"
	opSubscribeToConfigurationUpdate            = "aws.greengrass#SubscribeToConfigurationUpdate"
	opSubscribeToValidateConfigurationUpdates   = "aws.greengrass#SubscribeToValidateConfigurationUpdates"
	opCreateLocalDeployment                     = "aws.greengrass#CreateLocalDeployment"
	opCancelLocalDeployment                     = "aws.greengrass#CancelLocalDeployment"
	opGetLocalDeploymentStatus                  = "aws.greengrass#GetLocalDeploymentStatus"
	opListLocalDeployments                      = "aws.greengrass#ListLocalDeployments"
	opGetThingShadow                            = "aws.greengrass#GetThingShadow"
	opUpdateThingShadow                         = "aws.greengrass#UpdateThingShadow"
	opDeleteThingShadow                         = "aws.greengrass#DeleteThingShadow"
	opListNamedShadowsForThing                  = "aws.greengrass#ListNamedShadowsForThing"
	opGetSecretValue                            = "aws.greengrass#GetSecretValue"
	opAuthorizeClientDeviceAction               = "aws.greengrass#AuthorizeClientDeviceAction"
	opGetClientDeviceAuthToken                  = "aws.greengrass#GetClientDeviceAuthToken"
	opValidateAuthorizationToken                = "aws.greengrass#ValidateAuthorizationToken"
	opVerifyClientDeviceIdentity                = "aws.greengrass#VerifyClientDeviceIdentity"
	opSubscribeToCertificateUpdates             = "aws.greengrass#SubscribeToCertificateUpdates"
	opPutComponentMetric                        = "aws.greengrass#PutComponentMetric"
	opCreateDebugPassword                       = "aws.greengrass#CreateDebugPassword"
)

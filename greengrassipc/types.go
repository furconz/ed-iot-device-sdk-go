// Package greengrassipc provides a client for AWS IoT Greengrass IPC operations.
//
// This package implements the Greengrass Inter-Process Communication (IPC) protocol
// which allows Greengrass components to interact with the Greengrass nucleus and
// other AWS IoT services.
package greengrassipc

import (
	"encoding/base64"
	"encoding/json"
	"time"
)

// QOS represents the MQTT Quality of Service level
type QOS string

const (
	// QOSAtMostOnce - QoS 0, message delivered at most once, may be lost
	QOSAtMostOnce QOS = "0"

	// QOSAtLeastOnce - QoS 1, message delivered at least once, may be duplicated
	QOSAtLeastOnce QOS = "1"
)

// PayloadFormat represents the format of a message payload
type PayloadFormat string

const (
	// PayloadFormatBytes indicates a binary payload
	PayloadFormatBytes PayloadFormat = "BYTES"

	// PayloadFormatUTF8 indicates a UTF-8 text payload
	PayloadFormatUTF8 PayloadFormat = "UTF8"
)

// ReceiveMode defines how messages are received in subscriptions
type ReceiveMode string

const (
	// ReceiveModeReceiveAllMessages receives all messages
	ReceiveModeReceiveAllMessages ReceiveMode = "RECEIVE_ALL_MESSAGES"

	// ReceiveModeReceiveMessagesFromOthers only receives messages from other sources
	ReceiveModeReceiveMessagesFromOthers ReceiveMode = "RECEIVE_MESSAGES_FROM_OTHERS"
)

// LifecycleState represents the state of a component
type LifecycleState string

const (
	LifecycleStateRunning  LifecycleState = "RUNNING"
	LifecycleStateErrored  LifecycleState = "ERRORED"
	LifecycleStateNew      LifecycleState = "NEW"
	LifecycleStateFinished LifecycleState = "FINISHED"
	LifecycleStateInstalled LifecycleState = "INSTALLED"
	LifecycleStateBroken   LifecycleState = "BROKEN"
	LifecycleStateStarting LifecycleState = "STARTING"
	LifecycleStateStopping LifecycleState = "STOPPING"
)

// ReportedLifecycleState represents the lifecycle state that a component can report
type ReportedLifecycleState string

const (
	ReportedLifecycleStateRunning ReportedLifecycleState = "RUNNING"
	ReportedLifecycleStateErrored ReportedLifecycleState = "ERRORED"
)

// DeploymentStatus represents the status of a deployment
type DeploymentStatus string

const (
	DeploymentStatusQueued     DeploymentStatus = "QUEUED"
	DeploymentStatusInProgress DeploymentStatus = "IN_PROGRESS"
	DeploymentStatusSucceeded  DeploymentStatus = "SUCCEEDED"
	DeploymentStatusFailed     DeploymentStatus = "FAILED"
	DeploymentStatusCanceled   DeploymentStatus = "CANCELED"
)

// DetailedDeploymentStatus provides more detailed deployment status
type DetailedDeploymentStatus string

const (
	DetailedDeploymentStatusSuccessful                DetailedDeploymentStatus = "SUCCESSFUL"
	DetailedDeploymentStatusFailed                   DetailedDeploymentStatus = "FAILED_NO_STATE_CHANGE"
	DetailedDeploymentStatusFailedRollback          DetailedDeploymentStatus = "FAILED_ROLLBACK_NOT_REQUESTED"
	DetailedDeploymentStatusFailedRollbackComplete  DetailedDeploymentStatus = "FAILED_ROLLBACK_COMPLETE"
	DetailedDeploymentStatusRejected                DetailedDeploymentStatus = "REJECTED"
)

// FailureHandlingPolicy defines how deployment failures are handled
type FailureHandlingPolicy string

const (
	FailureHandlingPolicyRollback     FailureHandlingPolicy = "ROLLBACK"
	FailureHandlingPolicyDoNothing    FailureHandlingPolicy = "DO_NOTHING"
)

// RequestStatus represents the status of an operation request
type RequestStatus string

const (
	RequestStatusSucceeded RequestStatus = "SUCCEEDED"
	RequestStatusFailed    RequestStatus = "FAILED"
)

// ConfigurationValidityStatus represents whether a configuration is valid
type ConfigurationValidityStatus string

const (
	ConfigurationValidityStatusAccepted ConfigurationValidityStatus = "ACCEPTED"
	ConfigurationValidityStatusRejected ConfigurationValidityStatus = "REJECTED"
)

// CertificateType represents the type of certificate
type CertificateType string

const (
	CertificateTypeServer CertificateType = "SERVER"
)

// MetricUnitType represents the unit for a metric
type MetricUnitType string

const (
	MetricUnitTypeBytes        MetricUnitType = "BYTES"
	MetricUnitTypeBytesPerSecond MetricUnitType = "BYTES_PER_SECOND"
	MetricUnitTypeCount        MetricUnitType = "COUNT"
	MetricUnitTypeCountPerSecond MetricUnitType = "COUNT_PER_SECOND"
	MetricUnitTypeMegabytes    MetricUnitType = "MEGABYTES"
	MetricUnitTypeMegabytesPerSecond MetricUnitType = "MEGABYTES_PER_SECOND"
)

// BinaryMessage represents a binary message
type BinaryMessage struct {
	Message []byte `json:"message"`
}

// MarshalJSON implements custom JSON marshaling for BinaryMessage
func (b BinaryMessage) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{
		"message": base64.StdEncoding.EncodeToString(b.Message),
	})
}

// UnmarshalJSON implements custom JSON unmarshaling for BinaryMessage
func (b *BinaryMessage) UnmarshalJSON(data []byte) error {
	var temp struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}
	decoded, err := base64.StdEncoding.DecodeString(temp.Message)
	if err != nil {
		return err
	}
	b.Message = decoded
	return nil
}

// JsonMessage represents a JSON message
type JsonMessage struct {
	Message json.RawMessage `json:"message"`
}

// MQTTMessage represents an MQTT message
type MQTTMessage struct {
	TopicName      string                   `json:"topicName"`
	Payload        []byte                   `json:"payload,omitempty"`
	RetainOnline   *bool                     `json:"retain,omitempty"`
	UserProperties []UserProperty           `json:"userProperties,omitempty"`
	MessageExpiry  *int64                   `json:"messageExpiryIntervalSeconds,omitempty"`
	CorrelationData []byte                   `json:"correlationData,omitempty"`
	ResponseTopic  *string                  `json:"responseTopic,omitempty"`
	PayloadFormat  *PayloadFormat           `json:"payloadFormat,omitempty"`
	ContentType    *string                  `json:"contentType,omitempty"`
}

// MarshalJSON implements custom JSON marshaling for MQTTMessage
func (m MQTTMessage) MarshalJSON() ([]byte, error) {
	type Alias MQTTMessage
	return json.Marshal(&struct {
		Payload         *string `json:"payload,omitempty"`
		CorrelationData *string `json:"correlationData,omitempty"`
		*Alias
	}{
		Payload:         ptrString(base64.StdEncoding.EncodeToString(m.Payload)),
		CorrelationData: ptrString(base64.StdEncoding.EncodeToString(m.CorrelationData)),
		Alias:           (*Alias)(&m),
	})
}

// UnmarshalJSON implements custom JSON unmarshaling for MQTTMessage
func (m *MQTTMessage) UnmarshalJSON(data []byte) error {
	type Alias MQTTMessage
	aux := &struct {
		Payload         *string `json:"payload,omitempty"`
		CorrelationData *string `json:"correlationData,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(m),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.Payload != nil {
		decoded, err := base64.StdEncoding.DecodeString(*aux.Payload)
		if err != nil {
			return err
		}
		m.Payload = decoded
	}
	if aux.CorrelationData != nil {
		decoded, err := base64.StdEncoding.DecodeString(*aux.CorrelationData)
		if err != nil {
			return err
		}
		m.CorrelationData = decoded
	}
	return nil
}

// UserProperty represents an MQTT5 user property
type UserProperty struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// MessageContext provides context about a received message
type MessageContext struct {
	Topic string `json:"topic"`
}

// SystemResourceLimits defines resource limits for a component
type SystemResourceLimits struct {
	Memory *int64  `json:"memory,omitempty"` // In bytes
	CPUs   *float64 `json:"cpus,omitempty"`   // Fractional CPUs
}

// ComponentDetails provides information about a component
type ComponentDetails struct {
	ComponentName string                `json:"componentName"`
	Version       string                `json:"version"`
	State         LifecycleState        `json:"state"`
	Configuration map[string]interface{} `json:"configuration,omitempty"`
}


// DeploymentStatusDetails provides details about a deployment
type DeploymentStatusDetails struct {
	DetailedDeploymentStatus DetailedDeploymentStatus `json:"detailedDeploymentStatus"`
	DeploymentErrorStack     []string                 `json:"deploymentErrorStack,omitempty"`
	DeploymentErrorTypes     []string                 `json:"deploymentErrorTypes,omitempty"`
	DeploymentFailureCause   *string                  `json:"deploymentFailureCause,omitempty"`
}

// LocalDeployment represents a local deployment
type LocalDeployment struct {
	DeploymentID string           `json:"deploymentId"`
	Status       DeploymentStatus `json:"status"`
	CreatedOn    *time.Time       `json:"createdOn,omitempty"`
	DeploymentStatusDetails *DeploymentStatusDetails `json:"deploymentStatusDetails,omitempty"`
}

// MarshalJSON implements custom JSON marshaling for LocalDeployment
func (l LocalDeployment) MarshalJSON() ([]byte, error) {
	type Alias LocalDeployment
	var createdOn *float64
	if l.CreatedOn != nil {
		ts := float64(l.CreatedOn.UnixNano()) / 1e9
		createdOn = &ts
	}
	return json.Marshal(&struct {
		CreatedOn *float64 `json:"createdOn,omitempty"`
		*Alias
	}{
		CreatedOn: createdOn,
		Alias:     (*Alias)(&l),
	})
}

// UnmarshalJSON implements custom JSON unmarshaling for LocalDeployment
func (l *LocalDeployment) UnmarshalJSON(data []byte) error {
	type Alias LocalDeployment
	aux := &struct {
		CreatedOn *float64 `json:"createdOn,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(l),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.CreatedOn != nil {
		secs := int64(*aux.CreatedOn)
		nsecs := int64((*aux.CreatedOn - float64(secs)) * 1e9)
		t := time.Unix(secs, nsecs)
		l.CreatedOn = &t
	}
	return nil
}


// Metric represents a custom component metric
type Metric struct {
	Name      string         `json:"name"`
	Unit      MetricUnitType `json:"unit"`
	Value     float64        `json:"value"`
	Timestamp *time.Time     `json:"timestamp,omitempty"`
}

// MarshalJSON implements custom JSON marshaling for Metric
func (m Metric) MarshalJSON() ([]byte, error) {
	type Alias Metric
	var timestamp *float64
	if m.Timestamp != nil {
		ts := float64(m.Timestamp.UnixNano()) / 1e9
		timestamp = &ts
	}
	return json.Marshal(&struct {
		Timestamp *float64 `json:"timestamp,omitempty"`
		*Alias
	}{
		Timestamp: timestamp,
		Alias:     (*Alias)(&m),
	})
}

// UnmarshalJSON implements custom JSON unmarshaling for Metric
func (m *Metric) UnmarshalJSON(data []byte) error {
	type Alias Metric
	aux := &struct {
		Timestamp *float64 `json:"timestamp,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(m),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.Timestamp != nil {
		secs := int64(*aux.Timestamp)
		nsecs := int64((*aux.Timestamp - float64(secs)) * 1e9)
		t := time.Unix(secs, nsecs)
		m.Timestamp = &t
	}
	return nil
}

// ClientDeviceCredential represents credentials for a client device
type ClientDeviceCredential struct {
	ClientDeviceCertificate string `json:"clientDeviceCertificate"`
}

// CredentialDocument contains credential information
type CredentialDocument struct {
	MQTTCredential *MQTTCredential `json:"mqttCredential,omitempty"`
}

// MQTTCredential contains MQTT credentials
type MQTTCredential struct {
	ClientID     string `json:"clientId"`
	CertificatePem string `json:"certificatePem"`
	Username     *string `json:"username,omitempty"`
	Password     *string `json:"password,omitempty"`
}

// ValidateAuthorizationTokenRequest internal structure
type validateAuthorizationTokenData struct {
	Token string `json:"token"`
}

// Secret represents a secret value
type Secret struct {
	SecretID string                 `json:"secretId"`
	VersionID string                 `json:"versionId,omitempty"`
	VersionStage []string             `json:"versionStage,omitempty"`
	SecretValue *SecretValue         `json:"secretValue,omitempty"`
}

// SecretValue contains the actual secret data
type SecretValue struct {
	SecretString *string `json:"secretString,omitempty"`
	SecretBinary []byte  `json:"secretBinary,omitempty"`
}

// MarshalJSON implements custom JSON marshaling for SecretValue
func (s SecretValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		SecretString *string `json:"secretString,omitempty"`
		SecretBinary *string `json:"secretBinary,omitempty"`
	}{
		SecretString: s.SecretString,
		SecretBinary: ptrString(base64.StdEncoding.EncodeToString(s.SecretBinary)),
	})
}

// UnmarshalJSON implements custom JSON unmarshaling for SecretValue
func (s *SecretValue) UnmarshalJSON(data []byte) error {
	aux := &struct {
		SecretString *string `json:"secretString,omitempty"`
		SecretBinary *string `json:"secretBinary,omitempty"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	s.SecretString = aux.SecretString
	if aux.SecretBinary != nil {
		decoded, err := base64.StdEncoding.DecodeString(*aux.SecretBinary)
		if err != nil {
			return err
		}
		s.SecretBinary = decoded
	}
	return nil
}

// Helper functions
func ptrString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

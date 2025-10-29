package greengrassipc

// MQTT-related operations for communicating with AWS IoT Core

// PublishToIoTCoreRequest represents a request to publish to AWS IoT Core
type PublishToIoTCoreRequest struct {
	TopicName      string          `json:"topicName"`
	QOS            QOS             `json:"qos"`
	Payload        []byte          `json:"payload,omitempty"`
	Retain         *bool           `json:"retain,omitempty"`
	UserProperties []UserProperty  `json:"userProperties,omitempty"`
	MessageExpiry  *int64          `json:"messageExpiryIntervalSeconds,omitempty"`
	CorrelationData []byte         `json:"correlationData,omitempty"`
	ResponseTopic  *string         `json:"responseTopic,omitempty"`
	PayloadFormat  *PayloadFormat  `json:"payloadFormat,omitempty"`
	ContentType    *string         `json:"contentType,omitempty"`
}

// PublishToIoTCoreResponse represents the response from publishing to IoT Core
type PublishToIoTCoreResponse struct {
	// Empty response - operation success is indicated by lack of error
}

// SubscribeToIoTCoreRequest represents a request to subscribe to IoT Core topics
type SubscribeToIoTCoreRequest struct {
	TopicName string `json:"topicName"`
	QOS       QOS    `json:"qos"`
}

// SubscribeToIoTCoreResponse is the initial response for IoT Core subscription
type SubscribeToIoTCoreResponse struct {
	// Empty response - messages arrive via streaming
}

// IoTCoreMessage represents a message received from IoT Core
type IoTCoreMessage struct {
	Message *MQTTMessage `json:"message,omitempty"`
}

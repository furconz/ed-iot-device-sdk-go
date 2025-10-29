package greengrassipc

// Local pub/sub operations for inter-component communication

// PublishToTopicRequest represents a request to publish to a local topic
type PublishToTopicRequest struct {
	Topic           string             `json:"topic"`
	PublishMessage  *PublishMessage    `json:"publishMessage,omitempty"`
}

// PublishMessage contains the message to publish locally
type PublishMessage struct {
	BinaryMessage *BinaryMessage `json:"binaryMessage,omitempty"`
	JsonMessage   *JsonMessage   `json:"jsonMessage,omitempty"`
}

// PublishToTopicResponse represents the response from publishing locally
type PublishToTopicResponse struct {
	// Empty response - operation success is indicated by lack of error
}

// SubscribeToTopicRequest represents a request to subscribe to local topics
type SubscribeToTopicRequest struct {
	Topic       string      `json:"topic"`
	ReceiveMode ReceiveMode `json:"receiveMode,omitempty"`
}

// SubscribeToTopicResponse is the initial response for local topic subscription
type SubscribeToTopicResponse struct {
	TopicName *string `json:"topicName,omitempty"`
}

// SubscriptionResponseMessage represents a message received from a local subscription
type SubscriptionResponseMessage struct {
	BinaryMessage  *BinaryMessage  `json:"binaryMessage,omitempty"`
	JsonMessage    *JsonMessage    `json:"jsonMessage,omitempty"`
	MessageContext *MessageContext `json:"context,omitempty"`
}

package greengrassipc

// Configuration management operations

// GetConfigurationRequest represents a request to get component configuration
type GetConfigurationRequest struct {
	ComponentName *string  `json:"componentName,omitempty"`
	KeyPath       []string `json:"keyPath,omitempty"`
}

// GetConfigurationResponse contains the configuration
type GetConfigurationResponse struct {
	ComponentName *string                `json:"componentName,omitempty"`
	Value         map[string]interface{} `json:"value,omitempty"`
}

// UpdateConfigurationRequest represents a request to update configuration
type UpdateConfigurationRequest struct {
	KeyPath      []string               `json:"keyPath,omitempty"`
	Timestamp    *int64                 `json:"timestamp,omitempty"`
	ValueToMerge map[string]interface{} `json:"valueToMerge"`
}

// UpdateConfigurationResponse is the response from updating configuration
type UpdateConfigurationResponse struct {
	// Empty response
}

// SendConfigurationValidityReportRequest sends a configuration validity report
type SendConfigurationValidityReportRequest struct {
	ConfigurationValidityReport *ConfigurationValidityReport `json:"configurationValidityReport"`
}

// ConfigurationValidityReport contains validation results
type ConfigurationValidityReport struct {
	Status         ConfigurationValidityStatus `json:"status"`
	DeploymentID   string                      `json:"deploymentId"`
	Message        *string                     `json:"message,omitempty"`
}

// SendConfigurationValidityReportResponse is the response from sending report
type SendConfigurationValidityReportResponse struct {
	// Empty response
}

// SubscribeToConfigurationUpdateRequest subscribes to configuration updates
type SubscribeToConfigurationUpdateRequest struct {
	ComponentName *string  `json:"componentName,omitempty"`
	KeyPath       []string `json:"keyPath,omitempty"`
}

// SubscribeToConfigurationUpdateResponse is the initial response
type SubscribeToConfigurationUpdateResponse struct {
	// Empty response - updates arrive via streaming
}

// ConfigurationUpdateEvents represents configuration update events
type ConfigurationUpdateEvents struct {
	ConfigurationUpdateEvent *ConfigurationUpdateEvent `json:"configurationUpdateEvent,omitempty"`
}

// ConfigurationUpdateEvent contains the updated configuration
type ConfigurationUpdateEvent struct {
	ComponentName *string                `json:"componentName,omitempty"`
	KeyPath       []string               `json:"keyPath,omitempty"`
}

// SubscribeToValidateConfigurationUpdatesRequest subscribes to validate updates
type SubscribeToValidateConfigurationUpdatesRequest struct {
	// Empty request
}

// SubscribeToValidateConfigurationUpdatesResponse is the initial response
type SubscribeToValidateConfigurationUpdatesResponse struct {
	// Empty response - validation requests arrive via streaming
}

// ValidateConfigurationUpdateEvents represents validation events
type ValidateConfigurationUpdateEvents struct {
	ValidateConfigurationUpdateEvent *ValidateConfigurationUpdateEvent `json:"validateConfigurationUpdateEvent,omitempty"`
}

// ValidateConfigurationUpdateEvent contains the configuration to validate
type ValidateConfigurationUpdateEvent struct {
	Configuration map[string]interface{} `json:"configuration,omitempty"`
	DeploymentID  string                 `json:"deploymentId"`
}

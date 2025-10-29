package greengrassipc

// Secrets management operations

// GetSecretValueRequest represents a request to get a secret value
type GetSecretValueRequest struct {
	SecretID     string   `json:"secretId"`
	VersionID    *string  `json:"versionId,omitempty"`
	VersionStage *string  `json:"versionStage,omitempty"`
}

// GetSecretValueResponse contains the secret value
type GetSecretValueResponse struct {
	SecretID     string        `json:"secretId"`
	VersionID    string        `json:"versionId"`
	VersionStage []string      `json:"versionStage,omitempty"`
	SecretValue  *SecretValue  `json:"secretValue,omitempty"`
}

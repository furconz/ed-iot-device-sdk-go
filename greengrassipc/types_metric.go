package greengrassipc

// Metrics operations

// PutComponentMetricRequest publishes a custom component metric
type PutComponentMetricRequest struct {
	Metrics []Metric `json:"metrics"`
}

// PutComponentMetricResponse is the response from publishing metrics
type PutComponentMetricResponse struct {
	// Empty response
}

// CreateDebugPasswordRequest creates a debug password for SSH access
type CreateDebugPasswordRequest struct {
	// Empty request
}

// CreateDebugPasswordResponse contains the generated password
type CreateDebugPasswordResponse struct {
	Password         string  `json:"password"`
	Username         string  `json:"username"`
	PasswordExpiration *int64 `json:"passwordExpiration,omitempty"`
	CertificateSHA256Hash *string `json:"certificateSHA256Hash,omitempty"`
	CertificateSHA1Hash   *string `json:"certificateSHA1Hash,omitempty"`
}

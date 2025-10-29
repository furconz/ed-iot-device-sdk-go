package greengrassipc

// Certificate management operations

// SubscribeToCertificateUpdatesRequest subscribes to certificate updates
type SubscribeToCertificateUpdatesRequest struct {
	CertificateOptions *CertificateOptions `json:"certificateOptions,omitempty"`
}

// CertificateOptions specifies certificate options
type CertificateOptions struct {
	CertificateType CertificateType `json:"certificateType"`
}

// SubscribeToCertificateUpdatesResponse is the initial response
type SubscribeToCertificateUpdatesResponse struct {
	// Empty response - updates arrive via streaming
}

// CertificateUpdateEvent represents a certificate update event
type CertificateUpdateEvent struct {
	CertificateUpdate *CertificateUpdate `json:"certificateUpdate,omitempty"`
}

// CertificateUpdate provides information about a certificate update
type CertificateUpdate struct {
	CertificateAuthority string          `json:"certificateAuthority,omitempty"`
	Certificate          string          `json:"certificate,omitempty"`
	PrivateKey           string          `json:"privateKey,omitempty"`
	PublicKey            string          `json:"publicKey,omitempty"`
	CertificateType      CertificateType `json:"certificateType,omitempty"`
}

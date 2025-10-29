package greengrassipc

// Client device authentication operations

// AuthorizeClientDeviceActionRequest authorizes a client device action
type AuthorizeClientDeviceActionRequest struct {
	ClientDeviceAuthToken string  `json:"clientDeviceAuthToken"`
	Operation             string  `json:"operation"`
	Resource              string  `json:"resource"`
}

// AuthorizeClientDeviceActionResponse contains the authorization result
type AuthorizeClientDeviceActionResponse struct {
	IsAuthorized bool `json:"isAuthorized"`
}

// GetClientDeviceAuthTokenRequest gets an auth token for a client device
type GetClientDeviceAuthTokenRequest struct {
	Credential *ClientDeviceCredential `json:"credential"`
}

// GetClientDeviceAuthTokenResponse contains the auth token
type GetClientDeviceAuthTokenResponse struct {
	ClientDeviceAuthToken string `json:"clientDeviceAuthToken"`
}

// ValidateAuthorizationTokenRequest validates an authorization token
type ValidateAuthorizationTokenRequest struct {
	Token string `json:"token"`
}

// ValidateAuthorizationTokenResponse contains the validation result
type ValidateAuthorizationTokenResponse struct {
	IsValid bool `json:"isValid"`
}

// VerifyClientDeviceIdentityRequest verifies a client device identity
type VerifyClientDeviceIdentityRequest struct {
	Credential *ClientDeviceCredential `json:"credential"`
}

// VerifyClientDeviceIdentityResponse contains the verified identity
type VerifyClientDeviceIdentityResponse struct {
	IsValidClientDevice bool                `json:"isValidClientDevice"`
	ClientDeviceCredential *CredentialDocument `json:"clientDeviceCredential,omitempty"`
}

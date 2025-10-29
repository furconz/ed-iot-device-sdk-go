package greengrassipc

import "context"

// AuthorizeClientDeviceAction authorizes a client device action
//
// Checks whether a client device is authorized to perform a specific
// action on a resource.
func (c *Client) AuthorizeClientDeviceAction(ctx context.Context, req *AuthorizeClientDeviceActionRequest) (*AuthorizeClientDeviceActionResponse, error) {
	var resp AuthorizeClientDeviceActionResponse
	if err := c.requestResponse(ctx, opAuthorizeClientDeviceAction, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetClientDeviceAuthToken gets an authentication token for a client device
//
// Generates an authentication token that a client device can use to
// authenticate subsequent requests.
func (c *Client) GetClientDeviceAuthToken(ctx context.Context, req *GetClientDeviceAuthTokenRequest) (*GetClientDeviceAuthTokenResponse, error) {
	var resp GetClientDeviceAuthTokenResponse
	if err := c.requestResponse(ctx, opGetClientDeviceAuthToken, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ValidateAuthorizationToken validates an authorization token
//
// Validates whether an authorization token is valid and not expired.
func (c *Client) ValidateAuthorizationToken(ctx context.Context, req *ValidateAuthorizationTokenRequest) (*ValidateAuthorizationTokenResponse, error) {
	var resp ValidateAuthorizationTokenResponse
	if err := c.requestResponse(ctx, opValidateAuthorizationToken, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// VerifyClientDeviceIdentity verifies a client device's identity
//
// Verifies the identity of a client device using its credentials.
func (c *Client) VerifyClientDeviceIdentity(ctx context.Context, req *VerifyClientDeviceIdentityRequest) (*VerifyClientDeviceIdentityResponse, error) {
	var resp VerifyClientDeviceIdentityResponse
	if err := c.requestResponse(ctx, opVerifyClientDeviceIdentity, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

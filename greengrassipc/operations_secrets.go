package greengrassipc

import "context"

// GetSecretValue retrieves a secret from AWS Secrets Manager
//
// Gets the value of a secret that has been deployed to the Greengrass core.
// The component must have permission to access the secret.
func (c *Client) GetSecretValue(ctx context.Context, req *GetSecretValueRequest) (*GetSecretValueResponse, error) {
	var resp GetSecretValueResponse
	if err := c.requestResponse(ctx, opGetSecretValue, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

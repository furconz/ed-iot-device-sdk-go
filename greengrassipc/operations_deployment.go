package greengrassipc

import "context"

// CreateLocalDeployment creates a local deployment
//
// Creates a deployment that updates components on the local Greengrass core.
// This is useful for development and testing.
func (c *Client) CreateLocalDeployment(ctx context.Context, req *CreateLocalDeploymentRequest) (*CreateLocalDeploymentResponse, error) {
	var resp CreateLocalDeploymentResponse
	if err := c.requestResponse(ctx, opCreateLocalDeployment, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// CancelLocalDeployment cancels a local deployment
//
// Cancels an in-progress local deployment.
func (c *Client) CancelLocalDeployment(ctx context.Context, req *CancelLocalDeploymentRequest) (*CancelLocalDeploymentResponse, error) {
	var resp CancelLocalDeploymentResponse
	if err := c.requestResponse(ctx, opCancelLocalDeployment, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetLocalDeploymentStatus retrieves the status of a local deployment
//
// Returns the current status and details of the specified deployment.
func (c *Client) GetLocalDeploymentStatus(ctx context.Context, req *GetLocalDeploymentStatusRequest) (*GetLocalDeploymentStatusResponse, error) {
	var resp GetLocalDeploymentStatusResponse
	if err := c.requestResponse(ctx, opGetLocalDeploymentStatus, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListLocalDeployments lists all local deployments
//
// Returns a list of all local deployments on this Greengrass core.
func (c *Client) ListLocalDeployments(ctx context.Context, req *ListLocalDeploymentsRequest) (*ListLocalDeploymentsResponse, error) {
	var resp ListLocalDeploymentsResponse
	if err := c.requestResponse(ctx, opListLocalDeployments, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

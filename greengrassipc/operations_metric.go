package greengrassipc

import "context"

// PutComponentMetric publishes custom component metrics
//
// Allows components to publish custom metrics to Amazon CloudWatch.
// The component must have permission to publish metrics.
func (c *Client) PutComponentMetric(ctx context.Context, req *PutComponentMetricRequest) (*PutComponentMetricResponse, error) {
	var resp PutComponentMetricResponse
	if err := c.requestResponse(ctx, opPutComponentMetric, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// CreateDebugPassword creates a password for SSH debugging
//
// Generates a temporary password that can be used to SSH into the
// Greengrass core device for debugging purposes.
func (c *Client) CreateDebugPassword(ctx context.Context, req *CreateDebugPasswordRequest) (*CreateDebugPasswordResponse, error) {
	var resp CreateDebugPasswordResponse
	if err := c.requestResponse(ctx, opCreateDebugPassword, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

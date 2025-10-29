package greengrassipc

import "context"

// PublishToIoTCore publishes a message to AWS IoT Core
//
// This operation allows a component to publish messages to AWS IoT Core.
// The component must have the appropriate permissions in its configuration.
func (c *Client) PublishToIoTCore(ctx context.Context, req *PublishToIoTCoreRequest) (*PublishToIoTCoreResponse, error) {
	var resp PublishToIoTCoreResponse
	if err := c.requestResponse(ctx, opPublishToIoTCore, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

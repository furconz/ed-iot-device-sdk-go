package greengrassipc

import "context"

// PublishToTopic publishes a message to a local Greengrass topic
//
// This operation allows components to communicate with each other
// via local pub/sub without going through IoT Core.
func (c *Client) PublishToTopic(ctx context.Context, req *PublishToTopicRequest) (*PublishToTopicResponse, error) {
	var resp PublishToTopicResponse
	if err := c.requestResponse(ctx, opPublishToTopic, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

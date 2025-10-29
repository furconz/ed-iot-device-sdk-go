package greengrassipc

import "context"

// GetThingShadow retrieves a thing shadow
//
// Gets the shadow document for the specified thing. If shadowName is nil,
// retrieves the classic (unnamed) shadow.
func (c *Client) GetThingShadow(ctx context.Context, req *GetThingShadowRequest) (*GetThingShadowResponse, error) {
	var resp GetThingShadowResponse
	if err := c.requestResponse(ctx, opGetThingShadow, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// UpdateThingShadow updates a thing shadow
//
// Updates the shadow document for the specified thing. The payload should
// contain the shadow state to update (desired and/or reported).
func (c *Client) UpdateThingShadow(ctx context.Context, req *UpdateThingShadowRequest) (*UpdateThingShadowResponse, error) {
	var resp UpdateThingShadowResponse
	if err := c.requestResponse(ctx, opUpdateThingShadow, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// DeleteThingShadow deletes a thing shadow
//
// Deletes the shadow document for the specified thing.
func (c *Client) DeleteThingShadow(ctx context.Context, req *DeleteThingShadowRequest) (*DeleteThingShadowResponse, error) {
	var resp DeleteThingShadowResponse
	if err := c.requestResponse(ctx, opDeleteThingShadow, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListNamedShadowsForThing lists named shadows for a thing
//
// Returns the names of all named shadows for the specified thing.
func (c *Client) ListNamedShadowsForThing(ctx context.Context, req *ListNamedShadowsForThingRequest) (*ListNamedShadowsForThingResponse, error) {
	var resp ListNamedShadowsForThingResponse
	if err := c.requestResponse(ctx, opListNamedShadowsForThing, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

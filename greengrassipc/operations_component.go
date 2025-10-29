package greengrassipc

import "context"

// GetComponentDetails retrieves details about a component
//
// Returns information about the specified component including its
// version, state, and configuration.
func (c *Client) GetComponentDetails(ctx context.Context, req *GetComponentDetailsRequest) (*GetComponentDetailsResponse, error) {
	var resp GetComponentDetailsResponse
	if err := c.requestResponse(ctx, opGetComponentDetails, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListComponents lists all components running on the Greengrass core
//
// Returns information about all components including their versions,
// states, and configurations.
func (c *Client) ListComponents(ctx context.Context, req *ListComponentsRequest) (*ListComponentsResponse, error) {
	var resp ListComponentsResponse
	if err := c.requestResponse(ctx, opListComponents, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// RestartComponent restarts a component
//
// Stops and then starts the specified component. The component's
// process is terminated and restarted.
func (c *Client) RestartComponent(ctx context.Context, req *RestartComponentRequest) (*RestartComponentResponse, error) {
	var resp RestartComponentResponse
	if err := c.requestResponse(ctx, opRestartComponent, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// StopComponent stops a component
//
// Terminates the specified component's process. The component
// will not be automatically restarted.
func (c *Client) StopComponent(ctx context.Context, req *StopComponentRequest) (*StopComponentResponse, error) {
	var resp StopComponentResponse
	if err := c.requestResponse(ctx, opStopComponent, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// PauseComponent pauses a component
//
// Suspends the execution of the specified component.
func (c *Client) PauseComponent(ctx context.Context, req *PauseComponentRequest) (*PauseComponentResponse, error) {
	var resp PauseComponentResponse
	if err := c.requestResponse(ctx, opPauseComponent, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ResumeComponent resumes a paused component
//
// Resumes execution of a previously paused component.
func (c *Client) ResumeComponent(ctx context.Context, req *ResumeComponentRequest) (*ResumeComponentResponse, error) {
	var resp ResumeComponentResponse
	if err := c.requestResponse(ctx, opResumeComponent, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// UpdateState updates the component's reported lifecycle state
//
// Components should call this to report their state (RUNNING or ERRORED)
// to the Greengrass nucleus.
func (c *Client) UpdateState(ctx context.Context, req *UpdateStateRequest) (*UpdateStateResponse, error) {
	var resp UpdateStateResponse
	if err := c.requestResponse(ctx, opUpdateState, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// DeferComponentUpdate requests to defer a component update
//
// Allows a component to defer its update during a deployment,
// giving it time to finish critical work.
func (c *Client) DeferComponentUpdate(ctx context.Context, req *DeferComponentUpdateRequest) (*DeferComponentUpdateResponse, error) {
	var resp DeferComponentUpdateResponse
	if err := c.requestResponse(ctx, opDeferComponentUpdate, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

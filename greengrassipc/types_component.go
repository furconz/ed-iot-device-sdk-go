package greengrassipc

// Component lifecycle and management operations

// GetComponentDetailsRequest represents a request for component details
type GetComponentDetailsRequest struct {
	ComponentName string `json:"componentName"`
}

// GetComponentDetailsResponse contains the component details
type GetComponentDetailsResponse struct {
	ComponentDetails *ComponentDetails `json:"componentDetails,omitempty"`
}

// ListComponentsRequest represents a request to list components
type ListComponentsRequest struct {
	// Empty request
}

// ListComponentsResponse contains the list of components
type ListComponentsResponse struct {
	Components []ComponentDetails `json:"components,omitempty"`
}

// RestartComponentRequest represents a request to restart a component
type RestartComponentRequest struct {
	ComponentName string `json:"componentName"`
}

// RestartComponentResponse is the response from restarting a component
type RestartComponentResponse struct {
	RestartStatus RequestStatus `json:"restartStatus"`
	Message       *string       `json:"message,omitempty"`
}

// StopComponentRequest represents a request to stop a component
type StopComponentRequest struct {
	ComponentName string `json:"componentName"`
}

// StopComponentResponse is the response from stopping a component
type StopComponentResponse struct {
	StopStatus RequestStatus `json:"stopStatus"`
	Message    *string       `json:"message,omitempty"`
}

// PauseComponentRequest represents a request to pause a component
type PauseComponentRequest struct {
	ComponentName string `json:"componentName"`
}

// PauseComponentResponse is the response from pausing a component
type PauseComponentResponse struct {
	// Empty response
}

// ResumeComponentRequest represents a request to resume a paused component
type ResumeComponentRequest struct {
	ComponentName string `json:"componentName"`
}

// ResumeComponentResponse is the response from resuming a component
type ResumeComponentResponse struct {
	// Empty response
}

// UpdateStateRequest represents a request to update component state
type UpdateStateRequest struct {
	State ReportedLifecycleState `json:"state"`
}

// UpdateStateResponse is the response from updating state
type UpdateStateResponse struct {
	// Empty response
}

// DeferComponentUpdateRequest represents a request to defer a component update
type DeferComponentUpdateRequest struct {
	DeploymentID             string  `json:"deploymentId"`
	Message                  *string `json:"message,omitempty"`
	RecheckAfterMs           *int64  `json:"recheckAfterMs,omitempty"`
}

// DeferComponentUpdateResponse is the response from deferring an update
type DeferComponentUpdateResponse struct {
	// Empty response
}

// SubscribeToComponentUpdatesRequest subscribes to component update notifications
type SubscribeToComponentUpdatesRequest struct {
	// Empty request
}

// SubscribeToComponentUpdatesResponse is the initial response
type SubscribeToComponentUpdatesResponse struct {
	// Empty response - updates arrive via streaming
}

// ComponentUpdatePolicyEvents represents a component update event
type ComponentUpdatePolicyEvents struct {
	PreUpdateEvent  *PreComponentUpdateEvent  `json:"preUpdateEvent,omitempty"`
	PostUpdateEvent *PostComponentUpdateEvent `json:"postUpdateEvent,omitempty"`
}

// PreComponentUpdateEvent is sent before a component update
type PreComponentUpdateEvent struct {
	DeploymentID  string `json:"deploymentId"`
	IsGGCRestart  bool   `json:"isGgcRestart"`
}

// PostComponentUpdateEvent is sent after a component update
type PostComponentUpdateEvent struct {
	DeploymentID string `json:"deploymentId"`
}

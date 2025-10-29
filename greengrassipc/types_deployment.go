package greengrassipc

// Deployment management operations

// CreateLocalDeploymentRequest represents a request to create a local deployment
type CreateLocalDeploymentRequest struct {
	GroupName                     *string                          `json:"groupName,omitempty"`
	RootComponentVersionsToAdd    map[string]string                `json:"rootComponentVersionsToAdd,omitempty"`
	RootComponentsToRemove        []string                         `json:"rootComponentsToRemove,omitempty"`
	ComponentToConfiguration      map[string]map[string]interface{} `json:"componentToConfiguration,omitempty"`
	ComponentToRunWithInfo        map[string]RunWithInfo           `json:"componentToRunWithInfo,omitempty"`
	RecipeDirectoryPath           *string                          `json:"recipeDirectoryPath,omitempty"`
	ArtifactsDirectoryPath        *string                          `json:"artifactsDirectoryPath,omitempty"`
	FailureHandlingPolicy         *FailureHandlingPolicy           `json:"failureHandlingPolicy,omitempty"`
}

// RunWithInfo specifies how a component should run
type RunWithInfo struct {
	PosixUser          *string               `json:"posixUser,omitempty"`
	WindowsUser        *string               `json:"windowsUser,omitempty"`
	SystemResourceLimits *SystemResourceLimits `json:"systemResourceLimits,omitempty"`
}

// CreateLocalDeploymentResponse contains the deployment ID
type CreateLocalDeploymentResponse struct {
	DeploymentID string `json:"deploymentId"`
}

// CancelLocalDeploymentRequest represents a request to cancel a deployment
type CancelLocalDeploymentRequest struct {
	DeploymentID string `json:"deploymentId"`
}

// CancelLocalDeploymentResponse is the response from canceling
type CancelLocalDeploymentResponse struct {
	Message *string `json:"message,omitempty"`
}

// GetLocalDeploymentStatusRequest gets the status of a deployment
type GetLocalDeploymentStatusRequest struct {
	DeploymentID string `json:"deploymentId"`
}

// GetLocalDeploymentStatusResponse contains the deployment status
type GetLocalDeploymentStatusResponse struct {
	Deployment *LocalDeployment `json:"deployment,omitempty"`
}

// ListLocalDeploymentsRequest lists all local deployments
type ListLocalDeploymentsRequest struct {
	// Empty request
}

// ListLocalDeploymentsResponse contains the list of deployments
type ListLocalDeploymentsResponse struct {
	LocalDeployments []LocalDeployment `json:"localDeployments,omitempty"`
}

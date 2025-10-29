package greengrassipc

import "context"

// GetConfiguration retrieves the component's configuration
//
// Returns the configuration for the specified component, or the
// current component's configuration if no component name is specified.
func (c *Client) GetConfiguration(ctx context.Context, req *GetConfigurationRequest) (*GetConfigurationResponse, error) {
	var resp GetConfigurationResponse
	if err := c.requestResponse(ctx, opGetConfiguration, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// UpdateConfiguration updates the component's configuration
//
// Updates configuration values for the component. The nucleus
// merges the new values with existing configuration.
func (c *Client) UpdateConfiguration(ctx context.Context, req *UpdateConfigurationRequest) (*UpdateConfigurationResponse, error) {
	var resp UpdateConfigurationResponse
	if err := c.requestResponse(ctx, opUpdateConfiguration, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SendConfigurationValidityReport reports configuration validation results
//
// Components use this to report whether a proposed configuration
// change is valid (ACCEPTED) or invalid (REJECTED).
func (c *Client) SendConfigurationValidityReport(ctx context.Context, req *SendConfigurationValidityReportRequest) (*SendConfigurationValidityReportResponse, error) {
	var resp SendConfigurationValidityReportResponse
	if err := c.requestResponse(ctx, opSendConfigurationValidityReport, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

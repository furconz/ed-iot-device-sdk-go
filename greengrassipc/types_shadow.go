package greengrassipc

import "encoding/json"

// Device Shadow operations

// GetThingShadowRequest represents a request to get a thing shadow
type GetThingShadowRequest struct {
	ThingName  string  `json:"thingName"`
	ShadowName *string `json:"shadowName,omitempty"`
}

// GetThingShadowResponse contains the shadow document
type GetThingShadowResponse struct {
	Payload []byte `json:"payload,omitempty"`
}

// UpdateThingShadowRequest represents a request to update a thing shadow
type UpdateThingShadowRequest struct {
	ThingName  string  `json:"thingName"`
	ShadowName *string `json:"shadowName,omitempty"`
	Payload    []byte  `json:"payload,omitempty"`
}

// UpdateThingShadowResponse contains the updated shadow
type UpdateThingShadowResponse struct {
	Payload []byte `json:"payload,omitempty"`
}

// DeleteThingShadowRequest represents a request to delete a thing shadow
type DeleteThingShadowRequest struct {
	ThingName  string  `json:"thingName"`
	ShadowName *string `json:"shadowName,omitempty"`
}

// DeleteThingShadowResponse is the response from deleting a shadow
type DeleteThingShadowResponse struct {
	Payload []byte `json:"payload,omitempty"`
}

// ListNamedShadowsForThingRequest lists named shadows for a thing
type ListNamedShadowsForThingRequest struct {
	ThingName  string  `json:"thingName"`
	PageSize   *int32  `json:"pageSize,omitempty"`
	NextToken  *string `json:"nextToken,omitempty"`
}

// ListNamedShadowsForThingResponse contains the list of shadow names
type ListNamedShadowsForThingResponse struct {
	Results   []string `json:"results,omitempty"`
	Timestamp *int64   `json:"timestamp,omitempty"`
	NextToken *string  `json:"nextToken,omitempty"`
}

// ShadowState represents the state of a shadow (helper type)
type ShadowState struct {
	Desired  map[string]interface{} `json:"desired,omitempty"`
	Reported map[string]interface{} `json:"reported,omitempty"`
	Delta    map[string]interface{} `json:"delta,omitempty"`
}

// ShadowDocument represents a complete shadow document (helper type)
type ShadowDocument struct {
	State    *ShadowState           `json:"state,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	Version  *int64                 `json:"version,omitempty"`
	Timestamp *int64                `json:"timestamp,omitempty"`
}

// ParseShadowDocument is a helper to parse shadow payloads
func ParseShadowDocument(payload []byte) (*ShadowDocument, error) {
	var doc ShadowDocument
	if err := json.Unmarshal(payload, &doc); err != nil {
		return nil, err
	}
	return &doc, nil
}

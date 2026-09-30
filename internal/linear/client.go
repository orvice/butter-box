package linear

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// activity is the content of one Linear agent activity.
type activity struct {
	Type      string `json:"type"`
	Body      string `json:"body,omitempty"`
	Action    string `json:"action,omitempty"`
	Parameter string `json:"parameter,omitempty"`
	Result    string `json:"result,omitempty"`
	// Ephemeral activities are replaced by the next one; Linear only
	// accepts it on thought and action activities.
	Ephemeral bool `json:"-"`
}

func thought(body string) activity  { return activity{Type: "thought", Body: body} }
func response(body string) activity { return activity{Type: "response", Body: body} }
func failure(body string) activity  { return activity{Type: "error", Body: body} }

// client calls Linear's GraphQL API as the installed app.
type client struct {
	httpClient *http.Client
	url        string
	tokens     *tokens
}

// createActivity posts content to the agent session, authenticated as the
// app installation of the given workspace.
func (c *client) createActivity(ctx context.Context, organizationID, agentSessionID string, content activity) error {
	token, err := c.tokens.accessToken(ctx, organizationID)
	if err != nil {
		return err
	}
	input := map[string]any{
		"agentSessionId": agentSessionID,
		"content":        content,
	}
	if content.Ephemeral {
		input["ephemeral"] = true
	}
	var out struct {
		AgentActivityCreate struct {
			Success bool `json:"success"`
		} `json:"agentActivityCreate"`
	}
	err = c.queryWithToken(ctx, token, `mutation AgentActivityCreate($input: AgentActivityCreateInput!) {
  agentActivityCreate(input: $input) { success }
}`, map[string]any{"input": input}, &out)
	if err != nil {
		return err
	}
	if !out.AgentActivityCreate.Success {
		return errors.New("linear agentActivityCreate returned success=false")
	}
	return nil
}

// queryWithToken runs one GraphQL operation and decodes its data into out.
func (c *client) queryWithToken(ctx context.Context, token, query string, variables map[string]any, out any) error {
	payload, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("linear graphql: %w", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("linear graphql: %w", err)
	}

	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("linear graphql: HTTP %d with a non-JSON body", res.StatusCode)
	}
	if len(envelope.Errors) > 0 {
		return fmt.Errorf("linear graphql: %s", envelope.Errors[0].Message)
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("linear graphql: HTTP %d", res.StatusCode)
	}
	if len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return errors.New("linear graphql: response has no data")
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(envelope.Data, out); err != nil {
		return fmt.Errorf("linear graphql: decode data: %w", err)
	}
	return nil
}

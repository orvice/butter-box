package pi

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidModel = errors.New("invalid or unavailable model selection")
	ErrModelAuth    = errors.New("model authentication unavailable")
	ErrSetModel     = errors.New("model selection failed")
)

// ModelSelection identifies a model exactly, including its provider.
type ModelSelection struct {
	Provider string
	ModelID  string
}

// modelForSubmit delegates authentication, model_change recording, and
// thinking-level capability rules to Pi. It must run while holding runSlot.
func modelForSubmit(ctx context.Context, proc *process, selection *ModelSelection) (*ModelSelection, error) {
	if selection != nil {
		if strings.TrimSpace(selection.Provider) == "" || strings.TrimSpace(selection.ModelID) == "" {
			return nil, fmt.Errorf("%w: provider and model_id are required", ErrInvalidModel)
		}
		// Do not validate against the cached Box catalog. Pi's session-scoped
		// set_model performs exact matching and checks authentication, even
		// when the explicit selection matches the current model.
		resp, err := callRunCommand(ctx, proc, map[string]any{
			"type": "set_model", "provider": selection.Provider, "modelId": selection.ModelID,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrSetModel, err)
		}
		if !resp.Success {
			kind := ErrSetModel
			// Pi 1.1.0 returns string errors; unknown failures deliberately
			// stay set failures, never busy or authentication guesses.
			switch {
			case strings.HasPrefix(resp.Error, "Model not found:"):
				kind = ErrInvalidModel
			case strings.HasPrefix(resp.Error, "No API key for "):
				kind = ErrModelAuth
			}
			return nil, fmt.Errorf("%w: %s", kind, resp.Error)
		}
	}
	state, err := getState(ctx, proc)
	if err != nil {
		if selection != nil {
			return nil, fmt.Errorf("%w: verify Pi state: %w", ErrSetModel, err)
		}
		return nil, err
	}
	if selection != nil && (state.Model == nil || state.Model.Provider != selection.Provider || state.Model.ID != selection.ModelID) {
		return nil, fmt.Errorf("%w: Pi did not apply the requested provider/model_id", ErrSetModel)
	}
	if state.Model == nil {
		return nil, nil
	}
	return &ModelSelection{Provider: state.Model.Provider, ModelID: state.Model.ID}, nil
}

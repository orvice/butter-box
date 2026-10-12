package pi

import (
	"context"
	"encoding/json/v2"
	"fmt"
)

type commandOutcome struct {
	response response
	err      error
}

// pendingCommandError preserves completion of a native command whose response
// outlives the caller's deadline. Cancelling a waiter cannot cancel Pi.
type pendingCommandError struct {
	cause      error
	completion <-chan commandOutcome
}

func (e *pendingCommandError) Error() string { return e.cause.Error() }
func (e *pendingCommandError) Unwrap() error { return e.cause }

func callRunCommand(ctx context.Context, proc *process, cmd map[string]any) (response, error) {
	result := make(chan commandOutcome, 1)
	go func() {
		// Retain correlation until a definitive response or process exit.
		resp, err := proc.call(context.Background(), cmd)
		result <- commandOutcome{resp, err}
	}()
	select {
	case value := <-result:
		return value.response, value.err
	case <-ctx.Done():
		return response{}, &pendingCommandError{cause: ctx.Err(), completion: result}
	}
}

func callPrompt(ctx context.Context, proc *process, cmd map[string]any) (response, error) {
	resp, err := callRunCommand(ctx, proc, cmd)
	if err != nil {
		return response{}, fmt.Errorf("pi prompt: %w", err)
	}
	if !resp.Success {
		return resp, fmt.Errorf("pi prompt failed: %s", resp.Error)
	}
	return resp, nil
}

func promptHandled(resp response) bool {
	var data struct {
		Disposition string `json:"disposition"`
	}
	// Older Pi responses have no disposition; conservatively supervise a
	// run unless Pi explicitly says the input was handled without starting it.
	return json.Unmarshal(resp.Data, &data) == nil && data.Disposition == "handled"
}

package cursor

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	sdkv1 "github.com/orvice/butter-box/pkg/proto/sdk/v1"
)

const cursorAPIKeyErrorReason = "CURSOR_API_KEY_MISSING_OR_INVALID"

var (
	ErrBusy            = errors.New("cursor session is processing another message")
	ErrTooManySessions = errors.New("active cursor session limit reached")
	ErrNotFound        = errors.New("cursor session not found")
	ErrInvalidCwd      = errors.New("invalid cursor working directory")
	ErrInvalidMode     = errors.New("invalid cursor agent mode")
	ErrCancelled       = errors.New("cursor run cancelled")
	ErrRunIncomplete   = errors.New("cursor run ended without a terminal result")
	ErrAPIKey          = errors.New("cursor API key is missing or invalid")
)

// apiKeyError is deliberately stable: butter branches on this ErrorInfo
// reason to tell an operator to configure the key on the box. The API key is
// never included in the message or detail.
func apiKeyError() error {
	err := connect.NewError(connect.CodeUnauthenticated, ErrAPIKey)
	detail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Reason: cursorAPIKeyErrorReason,
	})
	if detailErr == nil {
		err.AddDetail(detail)
	}
	return err
}

func normalizeRPCError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	if detail, ok := sdkErrorDetail(err); ok {
		switch detail.GetSdkErrorCode() {
		case sdkv1.SdkErrorCode_SDK_ERROR_CODE_API_KEY_NOT_FOUND,
			sdkv1.SdkErrorCode_SDK_ERROR_CODE_UNAUTHORIZED:
			return apiKeyError()
		case sdkv1.SdkErrorCode_SDK_ERROR_CODE_AGENT_NOT_FOUND,
			sdkv1.SdkErrorCode_SDK_ERROR_CODE_RUN_NOT_FOUND:
			return ErrNotFound
		case sdkv1.SdkErrorCode_SDK_ERROR_CODE_AGENT_BUSY:
			return ErrBusy
		}
	}
	if reason := errorInfoReason(err); reason == cursorAPIKeyErrorReason {
		return apiKeyError()
	}

	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		switch connectErr.Code() {
		case connect.CodeNotFound:
			return ErrNotFound
		case connect.CodeResourceExhausted:
			return ErrTooManySessions
		case connect.CodeFailedPrecondition:
			return ErrBusy
		case connect.CodeUnauthenticated:
			// A bridge's own bearer failure is the bare "Unauthorized"
			// error. Do not turn that process-authentication problem into a
			// Cursor API-key detail.
			if looksLikeAPIKeyFailure(connectErr.Message()) {
				return apiKeyError()
			}
		}
	}
	return err
}

func sdkErrorDetail(err error) (*sdkv1.SdkErrorDetails, bool) {
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return nil, false
	}
	for _, detail := range connectErr.Details() {
		if detail.Type() != "sdk.v1.SdkErrorDetails" {
			continue
		}
		value, err := detail.Value()
		if err != nil {
			continue
		}
		parsed, ok := value.(*sdkv1.SdkErrorDetails)
		if ok {
			return parsed, true
		}
	}
	return nil, false
}

func errorInfoReason(err error) string {
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return ""
	}
	for _, detail := range connectErr.Details() {
		if detail.Type() != "google.rpc.ErrorInfo" {
			continue
		}
		value, err := detail.Value()
		if err != nil {
			continue
		}
		info, ok := value.(*errdetails.ErrorInfo)
		if ok {
			return info.GetReason()
		}
	}
	return ""
}

func looksLikeAPIKeyFailure(message string) bool {
	message = strings.ToLower(message)
	return strings.Contains(message, "api key") ||
		strings.Contains(message, "api_key") ||
		strings.Contains(message, "cursor_api_key") ||
		strings.Contains(message, "invalid user api key")
}

func runFailure(status, errorCode, statusMessage string) error {
	if looksLikeAPIKeyFailure(errorCode) || looksLikeAPIKeyFailure(statusMessage) {
		return apiKeyError()
	}

	switch status {
	case "cancelled", "canceled":
		return fmt.Errorf("%w: the bridge cancelled the run", ErrCancelled)
	case "error", "expired":
		if statusMessage == "" {
			statusMessage = "the bridge reported a failed run"
		}
		return fmt.Errorf("cursor run failed: %s", statusMessage)
	default:
		return fmt.Errorf("cursor run ended with status %q", status)
	}
}

package cursor

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	cursorv1 "github.com/orvice/butter-box/pkg/proto/butterbox/cursor/v1"
)

// Service implements cursorv1connect.CursorServiceHandler on top of a
// Manager.
type Service struct {
	manager *Manager
}

func NewService(manager *Manager) *Service {
	return &Service{manager: manager}
}

func (s *Service) CreateSession(ctx context.Context, req *connect.Request[cursorv1.CreateSessionRequest]) (*connect.Response[cursorv1.CreateSessionResponse], error) {
	id, err := s.manager.Create(ctx, CreateOpts{
		Name:  req.Msg.GetName(),
		Model: req.Msg.GetModel(),
		Mode:  req.Msg.GetMode(),
		Cwd:   req.Msg.GetCwd(),
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&cursorv1.CreateSessionResponse{SessionId: id}), nil
}

func (s *Service) SendMessage(ctx context.Context, req *connect.Request[cursorv1.SendMessageRequest]) (*connect.Response[cursorv1.SendMessageResponse], error) {
	text, err := s.manager.Send(ctx, req.Msg.GetSessionId(), req.Msg.GetMessage(), imagesInput(req.Msg.GetImages()))
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&cursorv1.SendMessageResponse{Text: text}), nil
}

func (s *Service) AbortSession(ctx context.Context, req *connect.Request[cursorv1.AbortSessionRequest]) (*connect.Response[cursorv1.AbortSessionResponse], error) {
	if err := s.manager.Abort(ctx, req.Msg.GetSessionId()); err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&cursorv1.AbortSessionResponse{}), nil
}

func (s *Service) ListModels(ctx context.Context, _ *connect.Request[cursorv1.ListModelsRequest]) (*connect.Response[cursorv1.ListModelsResponse], error) {
	models, err := s.manager.ListModels(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	response := &cursorv1.ListModelsResponse{}
	for _, model := range models {
		response.Models = append(response.Models, &cursorv1.Model{Id: model.ID, Name: model.Name})
	}
	return connect.NewResponse(response), nil
}

func rpcError(err error) error {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return err
	}
	switch {
	case errors.Is(err, ErrNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, ErrBusy):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, ErrTooManySessions):
		return connect.NewError(connect.CodeResourceExhausted, err)
	case errors.Is(err, ErrInvalidCwd), errors.Is(err, ErrInvalidMode):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, ErrCancelled), errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	case errors.Is(err, errBridgeExited):
		return connect.NewError(connect.CodeUnavailable, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

func imagesInput(images []*cursorv1.ImageContent) []ImageInput {
	if len(images) == 0 {
		return nil
	}
	out := make([]ImageInput, len(images))
	for i, image := range images {
		out[i] = ImageInput{
			MimeType: image.GetMimeType(),
			Data:     append([]byte(nil), image.GetData()...),
		}
	}
	return out
}

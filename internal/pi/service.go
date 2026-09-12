package pi

import (
	"context"
	"encoding/base64"
	"errors"
	"time"

	"connectrpc.com/connect"

	piv1 "github.com/orvice/butter-box/pkg/proto/butterbox/pi/v1"
)

// Service implements piv1connect.PiServiceHandler on top of a Manager.
type Service struct {
	manager *Manager
}

func NewService(manager *Manager) *Service {
	return &Service{manager: manager}
}

func (s *Service) CreateSession(ctx context.Context, req *connect.Request[piv1.CreateSessionRequest]) (*connect.Response[piv1.CreateSessionResponse], error) {
	info, err := s.manager.Create(ctx, CreateOpts{
		Name:          req.Msg.GetName(),
		Provider:      req.Msg.GetProvider(),
		Model:         req.Msg.GetModel(),
		ThinkingLevel: req.Msg.GetThinkingLevel(),
		Cwd:           req.Msg.GetCwd(),
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&piv1.CreateSessionResponse{Session: sessionProto(info)}), nil
}

func (s *Service) ListSessions(ctx context.Context, _ *connect.Request[piv1.ListSessionsRequest]) (*connect.Response[piv1.ListSessionsResponse], error) {
	infos, err := s.manager.List(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &piv1.ListSessionsResponse{}
	for _, info := range infos {
		resp.Sessions = append(resp.Sessions, sessionProto(info))
	}
	return connect.NewResponse(resp), nil
}

func (s *Service) ListEntries(ctx context.Context, req *connect.Request[piv1.ListEntriesRequest]) (*connect.Response[piv1.ListEntriesResponse], error) {
	result, err := s.manager.Entries(ctx, req.Msg.GetSessionId(), req.Msg.GetAfterCursor())
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &piv1.ListEntriesResponse{
		LeafId:  result.LeafID,
		Running: result.Running,
	}
	for _, e := range result.Entries {
		resp.Entries = append(resp.Entries, &piv1.Entry{
			Id:          e.ID,
			Type:        e.Type,
			PayloadJson: string(e.Raw),
		})
	}
	return connect.NewResponse(resp), nil
}

func (s *Service) GetSession(ctx context.Context, req *connect.Request[piv1.GetSessionRequest]) (*connect.Response[piv1.GetSessionResponse], error) {
	info, stats, err := s.manager.Get(ctx, req.Msg.GetSessionId())
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&piv1.GetSessionResponse{
		Session: sessionProto(info),
		Stats:   statsProto(stats),
	}), nil
}

func (s *Service) SendMessage(ctx context.Context, req *connect.Request[piv1.SendMessageRequest]) (*connect.Response[piv1.SendMessageResponse], error) {
	result, err := s.manager.Send(ctx, req.Msg.GetSessionId(), req.Msg.GetMessage(), imagesInput(req.Msg.GetImages()), nil)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&piv1.SendMessageResponse{
		Text:       result.Text,
		StopReason: result.StopReason,
		Stats:      statsProto(result.Stats),
	}), nil
}

func (s *Service) StreamMessage(ctx context.Context, req *connect.Request[piv1.StreamMessageRequest], stream *connect.ServerStream[piv1.StreamMessageResponse]) error {
	_, err := s.manager.Send(ctx, req.Msg.GetSessionId(), req.Msg.GetMessage(), imagesInput(req.Msg.GetImages()), func(eventType string, payload []byte) error {
		return stream.Send(&piv1.StreamMessageResponse{
			Type:        eventType,
			PayloadJson: string(payload),
		})
	})
	if err != nil {
		return rpcError(err)
	}
	return nil
}

func (s *Service) SubmitMessage(ctx context.Context, req *connect.Request[piv1.SubmitMessageRequest]) (*connect.Response[piv1.SubmitMessageResponse], error) {
	cursor, err := s.manager.Submit(ctx, req.Msg.GetSessionId(), req.Msg.GetMessage(), imagesInput(req.Msg.GetImages()))
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&piv1.SubmitMessageResponse{TurnCursor: cursor}), nil
}

func (s *Service) GetTurn(ctx context.Context, req *connect.Request[piv1.GetTurnRequest]) (*connect.Response[piv1.GetTurnResponse], error) {
	wait := time.Duration(req.Msg.GetWaitSeconds()) * time.Second
	status, err := s.manager.Turn(ctx, req.Msg.GetSessionId(), req.Msg.GetTurnCursor(), wait)
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &piv1.GetTurnResponse{Running: status.Running}
	if status.Result != nil {
		resp.Result = &piv1.TurnResult{
			Text:       status.Result.Text,
			StopReason: status.Result.StopReason,
			Stats:      statsProto(status.Result.Stats),
		}
	}
	return connect.NewResponse(resp), nil
}

func (s *Service) GetAvailableModels(ctx context.Context, req *connect.Request[piv1.GetAvailableModelsRequest]) (*connect.Response[piv1.GetAvailableModelsResponse], error) {
	models, err := s.manager.AvailableModels(ctx, req.Msg.GetSessionId())
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &piv1.GetAvailableModelsResponse{}
	for _, m := range models {
		resp.Models = append(resp.Models, &piv1.Model{
			Id:            m.ID,
			Provider:      m.Provider,
			Name:          m.Name,
			Api:           m.API,
			Reasoning:     m.Reasoning,
			Input:         m.Input,
			ContextWindow: m.ContextWindow,
			MaxTokens:     m.MaxTokens,
			Cost: &piv1.ModelCost{
				Input:      m.CostInput,
				Output:     m.CostOutput,
				CacheRead:  m.CostCacheRead,
				CacheWrite: m.CostCacheWrite,
			},
		})
	}
	return connect.NewResponse(resp), nil
}

func (s *Service) ListDirectories(_ context.Context, req *connect.Request[piv1.ListDirectoriesRequest]) (*connect.Response[piv1.ListDirectoriesResponse], error) {
	listing, err := s.manager.ListDirectories(req.Msg.GetPath(), req.Msg.GetIncludeHidden())
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &piv1.ListDirectoriesResponse{
		Path:      listing.Path,
		Truncated: listing.Truncated,
	}
	for _, dir := range listing.Directories {
		resp.Directories = append(resp.Directories, &piv1.Directory{
			Name: dir.Name,
			Path: dir.Path,
		})
	}
	return connect.NewResponse(resp), nil
}

func (s *Service) AbortSession(ctx context.Context, req *connect.Request[piv1.AbortSessionRequest]) (*connect.Response[piv1.AbortSessionResponse], error) {
	if err := s.manager.Abort(ctx, req.Msg.GetSessionId()); err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&piv1.AbortSessionResponse{}), nil
}

func (s *Service) DeleteSession(ctx context.Context, req *connect.Request[piv1.DeleteSessionRequest]) (*connect.Response[piv1.DeleteSessionResponse], error) {
	if err := s.manager.Delete(ctx, req.Msg.GetSessionId(), req.Msg.GetPurge()); err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&piv1.DeleteSessionResponse{}), nil
}

func rpcError(err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, ErrBusy):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, ErrTooManySessions):
		return connect.NewError(connect.CodeResourceExhausted, err)
	case errors.Is(err, ErrBadCursor), errors.Is(err, ErrInvalidCwd), errors.Is(err, ErrInvalidPath):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	default:
		var connectErr *connect.Error
		if errors.As(err, &connectErr) {
			return err
		}
		return connect.NewError(connect.CodeInternal, err)
	}
}

func sessionProto(info Info) *piv1.Session {
	updatedAt := int64(0)
	if !info.UpdatedAt.IsZero() {
		updatedAt = info.UpdatedAt.Unix()
	}
	return &piv1.Session{
		Id:            info.ID,
		Name:          info.Name,
		SessionFile:   info.File,
		Model:         info.Model,
		Streaming:     info.Streaming,
		MessageCount:  info.MessageCount,
		Cwd:           info.Cwd,
		Active:        info.Active,
		UpdatedAtUnix: updatedAt,
	}
}

func statsProto(stats Stats) *piv1.SessionStats {
	return &piv1.SessionStats{
		InputTokens:      stats.Input,
		OutputTokens:     stats.Output,
		CacheReadTokens:  stats.CacheRead,
		CacheWriteTokens: stats.CacheWrite,
		Cost:             stats.Cost,
		ContextPercent:   stats.ContextPercent,
	}
}

func imagesInput(images []*piv1.ImageContent) []ImageInput {
	if len(images) == 0 {
		return nil
	}
	out := make([]ImageInput, len(images))
	for i, img := range images {
		out[i] = ImageInput{
			MimeType:   img.GetMimeType(),
			Base64Data: base64.StdEncoding.EncodeToString(img.GetData()),
		}
	}
	return out
}

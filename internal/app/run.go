package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/orvice/butter-box/internal/cursor"
	"github.com/orvice/butter-box/internal/linear"
	"github.com/orvice/butter-box/internal/pi"
	cursorv1connect "github.com/orvice/butter-box/pkg/proto/butterbox/cursor/v1/cursorv1connect"
	"github.com/orvice/butter-box/pkg/proto/butterbox/pi/v1/piv1connect"
)

func Run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}

	server := NewMCPServer(cfg)
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return server
	}, &mcp.StreamableHTTPOptions{
		Stateless:    cfg.Stateless,
		JSONResponse: cfg.JSONResponse,
		Logger:       logger,
	})

	mux := http.NewServeMux()
	mux.Handle("GET /healthz", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"ok":true,"root":%q,"path":%q}`, cfg.Root, cfg.HTTPPath)
	}))
	mux.Handle(cfg.HTTPPath, WithBearerAuth(handler, cfg.Token))

	if cfg.PiWeb.Enabled {
		mux.Handle("/", NewPiWebHandler(cfg.PiWeb))
		StartPiWebProcess(ctx, logger, cfg.PiWeb)
	}

	// One pi session manager serves the Pi API and the Linear integration,
	// so both share the session cap and see each other's sessions.
	var manager *pi.Manager
	if cfg.PiAPI.Enabled || cfg.Linear.Enabled {
		manager = pi.NewManager(logger, pi.Config{
			Bin:         cfg.PiAPI.Bin,
			MaxSessions: cfg.PiAPI.MaxSessions,
			IdleTimeout: cfg.PiAPI.IdleTimeout,
			SessionDir:  cfg.PiAPI.SessionDir,
			SandboxRoot: cfg.Root,
		})
		defer manager.Stop()
	}

	if cfg.PiAPI.Enabled {
		path, handler := piv1connect.NewPiServiceHandler(pi.NewService(manager))
		mux.Handle(path, WithBearerAuth(handler, cfg.Token))
	}

	if cfg.Linear.Enabled {
		integration, err := linear.New(logger, linear.Config{
			ClientID:      cfg.Linear.ClientID,
			ClientSecret:  cfg.Linear.ClientSecret,
			WebhookSecret: cfg.Linear.WebhookSecret,
			InstallSecret: cfg.Linear.InstallSecret,
			BaseURL:       cfg.Linear.BaseURL,
			StateDir:      cfg.Linear.StateDir,
			Cwd:           cfg.Linear.Cwd,
			Provider:      cfg.Linear.Provider,
			Model:         cfg.Linear.Model,
			ThinkingLevel: cfg.Linear.ThinkingLevel,
			RunTimeout:    cfg.Linear.RunTimeout,
		}, manager)
		if err != nil {
			return fmt.Errorf("linear integration: %w", err)
		}
		// Deferred after manager.Stop, so it runs first: runs cut short by
		// the shutdown keep their marks for the next start to report.
		defer integration.Close()
		// Public routes: authenticated by Linear's webhook signature, the
		// install secret and the OAuth state, not the box bearer token.
		integration.Register(mux)
		integration.Start()
	}

	if cfg.Cursor.Enabled {
		manager := cursor.NewManager(logger, cursor.Config{
			Bin:         cfg.Cursor.Bin,
			APIKey:      cfg.Cursor.APIKey,
			MaxSessions: cfg.Cursor.MaxSessions,
			IdleTimeout: cfg.Cursor.IdleTimeout,
			SandboxRoot: cfg.Root,
		})
		defer manager.Stop()
		path, handler := cursorv1connect.NewCursorServiceHandler(cursor.NewService(manager))
		cursorToken := cfg.Cursor.AuthToken
		if cursorToken == "" {
			cursorToken = cfg.Token
		}
		mux.Handle(path, WithBearerAuth(handler, cursorToken))
	}

	httpServer := &http.Server{
		Addr:    cfg.Addr,
		Handler: mux,
	}

	logger.Info("butter box server starting",
		slog.String("addr", cfg.Addr),
		slog.String("mcp_path", cfg.HTTPPath),
		slog.String("sandbox_root", cfg.Root),
		slog.Bool("auth_enabled", cfg.Token != ""),
		slog.Bool("stateless", cfg.Stateless),
		slog.Bool("json_response", cfg.JSONResponse),
		slog.Bool("pi_web_enabled", cfg.PiWeb.Enabled),
		slog.Bool("pi_api_enabled", cfg.PiAPI.Enabled),
		slog.Bool("cursor_api_enabled", cfg.Cursor.Enabled),
		slog.Bool("linear_enabled", cfg.Linear.Enabled),
	)

	errCh := make(chan error, 1)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultToolTimeout)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return err
		}
		if err := <-errCh; err != nil {
			return err
		}
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

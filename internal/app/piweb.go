package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

const (
	piWebShutdownGrace     = 10 * time.Second
	piWebRestartResetAfter = time.Minute
	piWebMaxBackoff        = 30 * time.Second
)

// NewPiWebHandler proxies requests to the local pi-web instance. pi-web only
// supports root-path deployments, so this is mounted at "/"; the more
// specific /mcp and /healthz routes keep precedence on the mux. The outbound
// Host header is rewritten to the target so pi-web's hostname validation
// passes, and its own Basic Auth (PI_WEB_PASSWORD) travels through untouched
// in the Authorization header.
func NewPiWebHandler(cfg PiWebConfig) http.Handler {
	target := &url.URL{Scheme: "http", Host: fmt.Sprintf("127.0.0.1:%d", cfg.Port)}
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.SetXForwarded()
		},
		// Flush immediately: the UI streams agent output over long-lived
		// responses.
		FlushInterval: -1,
	}
}

// StartPiWebProcess launches pi-web as a supervised child process that
// restarts on failure and stops with ctx. PI_WEB_PASSWORD reaches the child
// through the inherited environment.
func StartPiWebProcess(ctx context.Context, logger *slog.Logger, cfg PiWebConfig) {
	go superviseProcess(ctx, logger, "pi-web", []string{
		"--hostname", "127.0.0.1",
		"--port", strconv.Itoa(cfg.Port),
		"--no-open",
	})
}

func superviseProcess(ctx context.Context, logger *slog.Logger, name string, args []string) {
	backoff := time.Second
	for ctx.Err() == nil {
		start := time.Now()

		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Env = os.Environ()
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Cancel = func() error {
			return cmd.Process.Signal(syscall.SIGTERM)
		}
		cmd.WaitDelay = piWebShutdownGrace

		logger.Info("starting supervised process", slog.String("cmd", name))
		err := cmd.Run()
		if ctx.Err() != nil {
			return
		}

		if time.Since(start) > piWebRestartResetAfter {
			backoff = time.Second
		}
		logger.Error("supervised process exited, restarting",
			slog.String("cmd", name),
			slog.Any("error", err),
			slog.Duration("backoff", backoff),
		)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < piWebMaxBackoff {
			backoff *= 2
		}
	}
}

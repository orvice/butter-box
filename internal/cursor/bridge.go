package cursor

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"

	sdkv1 "github.com/orvice/butter-box/pkg/proto/sdk/v1"
	sdkv1connect "github.com/orvice/butter-box/pkg/proto/sdk/v1/v1connect"
)

const (
	bridgeReadyPrefix       = "cursor-sdk-bridge ready "
	bridgeStartTimeout      = 30 * time.Second
	bridgeHealthTimeout     = 10 * time.Second
	bridgeShutdownTimeout   = 2 * time.Second
	bridgeProcessStopWait   = 5 * time.Second
	bridgeDiagnosticMaxLine = 4096
)

var errBridgeExited = errors.New("cursor bridge process exited")

type bridgeDiscovery struct {
	SchemaVersion int    `json:"schemaVersion"`
	ServerVersion string `json:"serverVersion"`
	Transport     string `json:"transport"`
	Protocol      string `json:"protocol"`
	Host          string `json:"host"`
	Port          int    `json:"port"`
	URL           string `json:"url"`
	AuthTokenFile string `json:"authTokenFile"`
	AuthToken     string `json:"authToken"`
}

type bridgeStartResult struct {
	discovery bridgeDiscovery
	err       error
}

// bridge owns one cursor-sdk-bridge process and its authenticated Connect
// clients. The bridge stores agent state durably, so stopping this process does
// not delete the session.
type bridge struct {
	logger *slog.Logger
	cmd    *exec.Cmd
	secret string

	control sdkv1connect.SdkBridgeControlServiceClient
	agent   sdkv1connect.SdkAgentServiceClient
	cursor  sdkv1connect.SdkCursorServiceClient

	done chan struct{}

	mu      sync.Mutex
	exitErr error

	stopOnce sync.Once
}

// startBridge launches a bridge, completes its stderr discovery handshake,
// and verifies that it speaks the expected sdk.v1 protocol.
func startBridge(ctx context.Context, logger *slog.Logger, bin, workspace, apiKey string) (*bridge, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if logger == nil {
		logger = slog.Default()
	}
	if workspace == "" {
		workspace, _ = os.Getwd()
	}

	cmd := exec.Command(bin, "--workspace", workspace)
	cmd.Dir = workspace
	cmd.Env = bridgeEnvironment(apiKey)
	cmd.Stdout = io.Discard
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("cursor bridge stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start cursor-sdk-bridge: %w", err)
	}

	b := &bridge{
		logger: logger,
		cmd:    cmd,
		secret: apiKey,
		done:   make(chan struct{}),
	}
	ready := make(chan bridgeStartResult, 1)
	go b.readStderr(stderr, ready)
	go b.wait()

	timer := time.NewTimer(bridgeStartTimeout)
	defer timer.Stop()

	var result bridgeStartResult
	select {
	case result = <-ready:
	case <-ctx.Done():
		b.stop()
		return nil, ctx.Err()
	case <-timer.C:
		b.stop()
		return nil, fmt.Errorf("timed out after %s waiting for cursor bridge ready line", bridgeStartTimeout)
	case <-b.done:
		// Wait normally follows the stderr EOF, but keep a small fallback for
		// unusual child processes that close descriptors in a different order.
		select {
		case result = <-ready:
		case <-time.After(100 * time.Millisecond):
			b.stop()
			return nil, errors.New("cursor bridge exited before emitting ready line")
		}
	}
	if result.err != nil {
		b.stop()
		return nil, result.err
	}

	if result.discovery.AuthToken == "" {
		b.stop()
		return nil, errors.New("cursor bridge discovery did not include an auth token")
	}

	httpClient := &http.Client{
		Transport: &bearerTransport{
			base:  http.DefaultTransport,
			token: result.discovery.AuthToken,
		},
	}
	b.control = sdkv1connect.NewSdkBridgeControlServiceClient(httpClient, result.discovery.URL)
	b.agent = sdkv1connect.NewSdkAgentServiceClient(httpClient, result.discovery.URL)
	b.cursor = sdkv1connect.NewSdkCursorServiceClient(httpClient, result.discovery.URL)

	healthCtx, cancel := context.WithTimeout(ctx, bridgeHealthTimeout)
	defer cancel()
	if _, err := b.control.Ping(healthCtx, connect.NewRequest(&sdkv1.PingRequest{})); err != nil {
		b.stop()
		return nil, fmt.Errorf("cursor bridge ping: %w", err)
	}
	version, err := b.control.GetVersion(healthCtx, connect.NewRequest(&sdkv1.GetVersionRequest{}))
	if err != nil {
		b.stop()
		return nil, fmt.Errorf("cursor bridge get version: %w", err)
	}
	if version.Msg.GetProtocolVersion() != "sdk.v1" {
		b.stop()
		return nil, fmt.Errorf("unsupported cursor bridge protocol %q", version.Msg.GetProtocolVersion())
	}

	return b, nil
}

func bridgeEnvironment(apiKey string) []string {
	env := os.Environ()
	for _, key := range []string{"CURSOR_API_KEY", "CURSOR_SDK_CLIENT_LANGUAGE"} {
		prefix := key + "="
		filtered := env[:0]
		for _, value := range env {
			if !strings.HasPrefix(value, prefix) {
				filtered = append(filtered, value)
			}
		}
		env = filtered
	}
	env = append(env, "CURSOR_API_KEY="+apiKey)
	env = append(env, "CURSOR_SDK_CLIENT_LANGUAGE=go")
	return env
}

// readStderr both finds the discovery line and keeps draining stderr for the
// lifetime of the bridge. Leaving the pipe unread can block a busy bridge.
func (b *bridge) readStderr(stderr io.ReadCloser, ready chan<- bridgeStartResult) {
	defer stderr.Close()
	scanner := bufio.NewScanner(stderr)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	var diagnostics []string
	readySent := false

	publish := func(result bridgeStartResult) {
		if readySent {
			return
		}
		readySent = true
		ready <- result
	}

	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if strings.HasPrefix(line, bridgeReadyPrefix) {
			var discovery bridgeDiscovery
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, bridgeReadyPrefix)), &discovery); err != nil {
				publish(bridgeStartResult{err: fmt.Errorf("invalid cursor bridge discovery JSON: %w", err)})
				continue
			}
			parsed, err := validateDiscovery(discovery)
			if err != nil {
				publish(bridgeStartResult{err: err})
				continue
			}
			publish(bridgeStartResult{discovery: parsed})
			continue
		}

		if line == "" {
			continue
		}
		line = redactSecret(line, b.secret)
		line = truncateDiagnostic(line)
		if len(diagnostics) < 32 {
			diagnostics = append(diagnostics, line)
		}
		b.logger.Debug("cursor bridge diagnostic", slog.String("line", line))
	}

	if readySent {
		return
	}
	err := errors.New("cursor bridge exited before emitting ready line")
	if scanErr := scanner.Err(); scanErr != nil {
		err = fmt.Errorf("%w: read stderr: %v", err, scanErr)
	}
	if len(diagnostics) > 0 {
		err = fmt.Errorf("%w: %s", err, strings.Join(diagnostics, "\n"))
	}
	publish(bridgeStartResult{err: err})
}

func validateDiscovery(discovery bridgeDiscovery) (bridgeDiscovery, error) {
	if discovery.SchemaVersion != 1 {
		return bridgeDiscovery{}, fmt.Errorf("unsupported cursor bridge discovery schema %d", discovery.SchemaVersion)
	}
	if discovery.Transport != "tcp" || discovery.Protocol != "connect" {
		return bridgeDiscovery{}, fmt.Errorf("unsupported cursor bridge discovery transport=%q protocol=%q", discovery.Transport, discovery.Protocol)
	}
	if discovery.URL == "" {
		if discovery.Host == "" || discovery.Port <= 0 || discovery.Port > 65535 {
			return bridgeDiscovery{}, errors.New("cursor bridge discovery has no valid URL or host/port")
		}
		discovery.URL = "http://" + net.JoinHostPort(discovery.Host, strconv.Itoa(discovery.Port))
	}
	u, err := url.Parse(discovery.URL)
	if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return bridgeDiscovery{}, fmt.Errorf("invalid cursor bridge URL %q", discovery.URL)
	}

	if discovery.AuthToken == "" {
		if discovery.AuthTokenFile == "" {
			return bridgeDiscovery{}, errors.New("cursor bridge discovery has no auth token file")
		}
		data, err := os.ReadFile(discovery.AuthTokenFile)
		if err != nil {
			return bridgeDiscovery{}, fmt.Errorf("read cursor bridge auth token file: %w", err)
		}
		discovery.AuthToken = strings.TrimSpace(string(data))
	}
	if discovery.AuthToken == "" {
		return bridgeDiscovery{}, errors.New("cursor bridge auth token is empty")
	}
	return discovery, nil
}

func redactSecret(value, secret string) string {
	if secret == "" {
		return value
	}
	return strings.ReplaceAll(value, secret, "[REDACTED]")
}

func truncateDiagnostic(line string) string {
	if len(line) <= bridgeDiagnosticMaxLine {
		return line
	}
	return line[:bridgeDiagnosticMaxLine] + "..."
}

func (b *bridge) wait() {
	err := b.cmd.Wait()
	b.mu.Lock()
	if err == nil {
		b.exitErr = errBridgeExited
	} else {
		b.exitErr = fmt.Errorf("%w: %v", errBridgeExited, err)
	}
	b.mu.Unlock()
	close(b.done)
}

func (b *bridge) exitError() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.exitErr != nil {
		return b.exitErr
	}
	return errBridgeExited
}

// stop asks the bridge to shut down gracefully, then escalates to SIGTERM and
// SIGKILL. It is safe to call from multiple goroutines.
func (b *bridge) stop() {
	b.stopOnce.Do(func() {
		if b.control != nil {
			ctx, cancel := context.WithTimeout(context.Background(), bridgeShutdownTimeout)
			_, err := b.control.Shutdown(ctx, connect.NewRequest(&sdkv1.ShutdownRequest{}))
			cancel()
			if err != nil && b.cmd.Process != nil {
				_ = b.cmd.Process.Signal(syscall.SIGTERM)
			}
		} else if b.cmd.Process != nil {
			_ = b.cmd.Process.Signal(syscall.SIGTERM)
		}

		select {
		case <-b.done:
		case <-time.After(bridgeProcessStopWait):
			if b.cmd.Process != nil {
				_ = b.cmd.Process.Kill()
			}
			<-b.done
		}
	})
	<-b.done
}

func (b *bridge) cancelRun(ctx context.Context, runID string) error {
	if runID == "" {
		return nil
	}
	_, err := b.agent.CancelRun(ctx, connect.NewRequest(&sdkv1.CancelRunRequest{RunId: runID}))
	return err
}

type bearerTransport struct {
	base  http.RoundTripper
	token string
}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header.Set("Authorization", "Bearer "+t.token)
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(clone)
}

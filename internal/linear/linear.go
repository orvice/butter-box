// Package linear runs pi as a Linear agent: Linear delivers AgentSessionEvent
// webhooks, each Linear agent session is backed by one pi session on the
// box, and progress and results are posted back as agent activities.
//
// The routes it serves are reached by Linear and a browser rather than a
// ButterBox client, so they do not use the box bearer token: the webhook is
// authenticated by Linear's signature, the install by its own secret, and
// the OAuth callback by a single-use state.
package linear

import (
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

// Routes served by the integration.
const (
	InstallPath  = "/linear/install"
	CallbackPath = "/linear/oauth/callback"
	WebhookPath  = "/linear/webhook"
)

// Config configures the Linear integration.
type Config struct {
	ClientID      string
	ClientSecret  string
	WebhookSecret string
	// InstallSecret gates InstallPath, so strangers cannot install their
	// workspace into this box.
	InstallSecret string
	// BaseURL is the box's public URL; the OAuth redirect URI is derived
	// from it and must be registered with the Linear app.
	BaseURL string
	// StateDir holds OAuth installations and the Linear-to-pi session map.
	StateDir string

	// Cwd, Provider, Model and ThinkingLevel configure the pi sessions
	// created for Linear sessions; Cwd is resolved like the Pi API's.
	Cwd           string
	Provider      string
	Model         string
	ThinkingLevel string
	// RunTimeout bounds one run; pi is aborted when it expires.
	RunTimeout time.Duration
}

func (c Config) redirectURI() string {
	return strings.TrimRight(c.BaseURL, "/") + CallbackPath
}

// Integration serves the Linear routes and drives the sessions behind them.
type Integration struct {
	cfg        Config
	logger     *slog.Logger
	endpoints  endpoints
	now        func() time.Time
	tokens     *tokens
	client     *client
	states     *oauthStates
	deliveries *deliveries
	runner     *runner
	// dispatch receives authenticated agent session events.
	dispatch func(sessionEvent)
}

// New opens the integration's state under cfg.StateDir.
func New(logger *slog.Logger, cfg Config, agent Agent) (*Integration, error) {
	return newIntegration(logger, cfg, agent, defaultEndpoints, &http.Client{Timeout: 30 * time.Second})
}

func newIntegration(logger *slog.Logger, cfg Config, agent Agent, eps endpoints, httpClient *http.Client) (*Integration, error) {
	installs, err := openFileStore[installation](filepath.Join(cfg.StateDir, "installations.json"))
	if err != nil {
		return nil, err
	}
	sessions, err := openFileStore[sessionRecord](filepath.Join(cfg.StateDir, "sessions.json"))
	if err != nil {
		return nil, err
	}

	i := &Integration{
		cfg:        cfg,
		logger:     logger,
		endpoints:  eps,
		now:        time.Now,
		states:     &oauthStates{states: map[string]time.Time{}},
		deliveries: &deliveries{seen: map[string]time.Time{}},
	}
	i.tokens = &tokens{
		cfg:        cfg,
		endpoints:  eps,
		httpClient: httpClient,
		store:      installs,
		now:        time.Now,
	}
	i.client = &client{httpClient: httpClient, url: eps.GraphQLURL, tokens: i.tokens}
	i.runner = &runner{
		agent:            agent,
		post:             i.client.createActivity,
		sessions:         sessions,
		cfg:              cfg,
		logger:           logger,
		now:              time.Now,
		progressInterval: defaultProgressInterval,
		active:           map[string]*runState{},
	}
	i.dispatch = i.runner.handle
	return i, nil
}

// Register mounts the Linear routes on mux.
func (i *Integration) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET "+InstallPath, i.handleInstall)
	mux.HandleFunc("GET "+CallbackPath, i.handleCallback)
	mux.HandleFunc("POST "+WebhookPath, i.handleWebhook)
}

// Start tells Linear about runs a previous box process left unfinished. The
// run marks are cleared before it returns; the notices post in the
// background.
func (i *Integration) Start() {
	interrupted := i.runner.interrupted()
	if len(interrupted) == 0 {
		return
	}
	go func() {
		for id, rec := range interrupted {
			i.logger.Info("reporting interrupted linear run", slog.String("agent_session_id", id))
			i.runner.notify(rec.OrganizationID, id, failure(
				"ButterBox restarted before this run finished. The pi session and its history are kept: send a message to continue."))
		}
	}()
}

// Close stops new runs from starting.
func (i *Integration) Close() {
	i.runner.close()
}

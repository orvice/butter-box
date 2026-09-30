package linear

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orvice/butter-box/internal/pi"
)

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		ClientID:      "client-id",
		ClientSecret:  "client-secret",
		WebhookSecret: "webhook-secret",
		InstallSecret: "install-secret-0123456789",
		BaseURL:       "https://box.example.com/",
		StateDir:      t.TempDir(),
		Cwd:           "repo",
		RunTimeout:    time.Minute,
	}
}

// newTestIntegration builds an integration against fl (nil: Linear is never
// reached, so tests must replace the runner's post). Progress posts are
// near-immediate.
func newTestIntegration(t *testing.T, cfg Config, agent Agent, fl *fakeLinear) *Integration {
	t.Helper()
	eps := endpoints{AuthorizeURL: "https://linear.invalid/oauth/authorize", TokenURL: "https://linear.invalid/token", GraphQLURL: "https://linear.invalid/graphql"}
	if fl != nil {
		eps = fl.endpoints()
	}
	i, err := newIntegration(testLogger(t), cfg, agent, eps, http.DefaultClient)
	if err != nil {
		t.Fatalf("newIntegration: %v", err)
	}
	i.runner.progressInterval = 5 * time.Millisecond
	t.Cleanup(func() {
		i.Close()
		i.runner.wg.Wait()
	})
	return i
}

// fakeLinear is a minimal Linear API: the OAuth token endpoint and the
// GraphQL operations the integration uses.
type fakeLinear struct {
	t   *testing.T
	srv *httptest.Server

	mu         sync.Mutex
	grants     []url.Values
	activities []postedActivity
	issued     int
	expiresIn  int64
}

type postedActivity struct {
	Token string
	Input map[string]any
}

func newFakeLinear(t *testing.T) *fakeLinear {
	t.Helper()
	fl := &fakeLinear{t: t, expiresIn: 86399}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /oauth/token", fl.handleToken)
	mux.HandleFunc("POST /graphql", fl.handleGraphQL)
	fl.srv = httptest.NewServer(mux)
	t.Cleanup(fl.srv.Close)
	return fl
}

func (fl *fakeLinear) endpoints() endpoints {
	return endpoints{
		AuthorizeURL: "https://linear.app/oauth/authorize",
		TokenURL:     fl.srv.URL + "/oauth/token",
		GraphQLURL:   fl.srv.URL + "/graphql",
	}
}

func (fl *fakeLinear) handleToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fl.mu.Lock()
	fl.grants = append(fl.grants, r.PostForm)
	fl.issued++
	n, expires := fl.issued, fl.expiresIn
	fl.mu.Unlock()

	if r.PostForm.Get("client_secret") != "client-secret" {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_client", "error_description": "bad client"})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token":  fmt.Sprintf("access-%d", n),
		"refresh_token": fmt.Sprintf("refresh-%d", n),
		"token_type":    "Bearer",
		"expires_in":    expires,
		"scope":         "read write app:assignable app:mentionable",
	})
}

func (fl *fakeLinear) handleGraphQL(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	body, _ := io.ReadAll(r.Body)
	var req struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch {
	case strings.Contains(req.Query, "agentActivityCreate"):
		input, _ := req.Variables["input"].(map[string]any)
		fl.mu.Lock()
		fl.activities = append(fl.activities, postedActivity{Token: token, Input: input})
		fl.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"agentActivityCreate": map[string]any{"success": true}}})
	case strings.Contains(req.Query, "viewer"):
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
			"viewer":       map[string]any{"id": "app-user-1"},
			"organization": map[string]any{"id": "org-1", "name": "Acme"},
		}})
	default:
		_ = json.NewEncoder(w).Encode(map[string]any{"errors": []map[string]any{{"message": "unknown operation"}}})
	}
}

func (fl *fakeLinear) snapshot() ([]url.Values, []postedActivity) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	return append([]url.Values(nil), fl.grants...), append([]postedActivity(nil), fl.activities...)
}

// fakeAgent stands in for pi.Manager.
type fakeAgent struct {
	mu        sync.Mutex
	creates   []pi.CreateOpts
	sends     []sentPrompt
	aborts    []string
	created   int
	createErr error
	// sendFn runs one Send; nil replies "done: <message>" at once.
	sendFn func(ctx context.Context, message string, onEvent func(string, []byte) error) (pi.SendResult, error)
	// started receives the message of each Send as it begins.
	started chan string
}

type sentPrompt struct {
	ID      string
	Message string
}

func newFakeAgent() *fakeAgent {
	return &fakeAgent{started: make(chan string, 16)}
}

func (a *fakeAgent) Create(_ context.Context, opts pi.CreateOpts) (pi.Info, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.creates = append(a.creates, opts)
	if a.createErr != nil {
		return pi.Info{}, a.createErr
	}
	a.created++
	return pi.Info{ID: fmt.Sprintf("pi-%d", a.created), Cwd: opts.Cwd}, nil
}

func (a *fakeAgent) Send(ctx context.Context, id, message string, _ []pi.ImageInput, onEvent func(string, []byte) error) (pi.SendResult, error) {
	a.mu.Lock()
	a.sends = append(a.sends, sentPrompt{ID: id, Message: message})
	fn := a.sendFn
	a.mu.Unlock()
	a.started <- message
	if fn == nil {
		return pi.SendResult{Text: "done: " + message, StopReason: "stop"}, nil
	}
	return fn(ctx, message, onEvent)
}

func (a *fakeAgent) Abort(_ context.Context, id string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.aborts = append(a.aborts, id)
	return nil
}

func (a *fakeAgent) snapshot() ([]pi.CreateOpts, []sentPrompt, []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]pi.CreateOpts(nil), a.creates...), append([]sentPrompt(nil), a.sends...), append([]string(nil), a.aborts...)
}

func (a *fakeAgent) waitStarted(t *testing.T) string {
	t.Helper()
	select {
	case msg := <-a.started:
		return msg
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a pi run to start")
		return ""
	}
}

// recorder captures the activities a runner posts.
type recorder struct {
	mu  sync.Mutex
	got []posted
	ch  chan posted
}

type posted struct {
	Org     string
	Session string
	activity
}

func newRecorder() *recorder {
	return &recorder{ch: make(chan posted, 64)}
}

func (r *recorder) post(_ context.Context, org, session string, a activity) error {
	p := posted{Org: org, Session: session, activity: a}
	r.mu.Lock()
	r.got = append(r.got, p)
	r.mu.Unlock()
	r.ch <- p
	return nil
}

func (r *recorder) all() []posted {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]posted(nil), r.got...)
}

// next waits for the next activity of the given type, skipping others.
func (r *recorder) next(t *testing.T, typ string) posted {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case p := <-r.ch:
			if p.Type == typ {
				return p
			}
		case <-timeout:
			t.Fatalf("timed out waiting for a %s activity; got %+v", typ, r.all())
			return posted{}
		}
	}
}

// withRecorder swaps the runner's Linear poster for r.
func withRecorder(i *Integration) *recorder {
	r := newRecorder()
	i.runner.post = r.post
	return r
}

func createdEvent(sessionID string) sessionEvent {
	var ev sessionEvent
	ev.Type = "AgentSessionEvent"
	ev.Action = "created"
	ev.OrganizationID = "org-1"
	ev.PromptContext = "<issue identifier=\"ENG-1\">Fix login</issue>"
	ev.AgentSession.ID = sessionID
	ev.AgentSession.Issue = &issueRef{Identifier: "ENG-1", Title: "Fix login"}
	return ev
}

func promptedEvent(sessionID, body, signal string) sessionEvent {
	var ev sessionEvent
	ev.Type = "AgentSessionEvent"
	ev.Action = "prompted"
	ev.OrganizationID = "org-1"
	ev.AgentSession.ID = sessionID
	ev.AgentActivity = &promptActivity{Signal: signal}
	ev.AgentActivity.Content.Body = body
	return ev
}

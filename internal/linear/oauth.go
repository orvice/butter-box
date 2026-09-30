package linear

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	// oauthScopes are what an assignable, mentionable agent app needs.
	oauthScopes = "read,write,app:assignable,app:mentionable"
	stateTTL    = 10 * time.Minute
	// refreshSkew refreshes an access token this long before it expires.
	refreshSkew = 5 * time.Minute
)

// endpoints are Linear's API URLs, overridable in tests.
type endpoints struct {
	AuthorizeURL string
	TokenURL     string
	GraphQLURL   string
}

var defaultEndpoints = endpoints{
	AuthorizeURL: "https://linear.app/oauth/authorize",
	TokenURL:     "https://api.linear.app/oauth/token",
	GraphQLURL:   "https://api.linear.app/graphql",
}

// tokenResponse is the OAuth token endpoint's answer.
type tokenResponse struct {
	AccessToken      string          `json:"access_token"`
	RefreshToken     string          `json:"refresh_token"`
	ExpiresIn        int64           `json:"expires_in"`
	Scope            json.RawMessage `json:"scope"`
	Error            string          `json:"error"`
	ErrorDescription string          `json:"error_description"`
}

// scopeString normalizes scope, which Linear may send as a string or a list.
func (t tokenResponse) scopeString() string {
	var s string
	if json.Unmarshal(t.Scope, &s) == nil {
		return s
	}
	var list []string
	if json.Unmarshal(t.Scope, &list) == nil {
		return strings.Join(list, ",")
	}
	return ""
}

func (t tokenResponse) expiresAt(now time.Time) time.Time {
	if t.ExpiresIn <= 0 {
		return time.Time{}
	}
	return now.Add(time.Duration(t.ExpiresIn) * time.Second)
}

// tokens hands out access tokens per Linear workspace, refreshing them
// before they expire.
type tokens struct {
	cfg        Config
	endpoints  endpoints
	httpClient *http.Client
	store      *fileStore[installation]
	now        func() time.Time

	refreshMu sync.Mutex // serializes refreshes: refresh tokens rotate
}

// accessToken returns a valid token for the workspace. An empty
// organizationID selects the only installation, if there is exactly one.
func (t *tokens) accessToken(ctx context.Context, organizationID string) (string, error) {
	t.refreshMu.Lock()
	defer t.refreshMu.Unlock()

	inst, err := t.installation(organizationID)
	if err != nil {
		return "", err
	}
	if inst.ExpiresAt.IsZero() || t.now().Add(refreshSkew).Before(inst.ExpiresAt) {
		return inst.AccessToken, nil
	}
	if inst.RefreshToken == "" {
		return "", fmt.Errorf("linear access token for workspace %s expired and no refresh token is stored; reinstall via %s", inst.OrganizationID, InstallPath)
	}

	resp, err := t.exchange(ctx, url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {inst.RefreshToken},
		"client_id":     {t.cfg.ClientID},
		"client_secret": {t.cfg.ClientSecret},
	})
	if err != nil {
		return "", fmt.Errorf("refresh linear token: %w", err)
	}
	now := t.now()
	inst.AccessToken = resp.AccessToken
	if resp.RefreshToken != "" {
		inst.RefreshToken = resp.RefreshToken
	}
	if scope := resp.scopeString(); scope != "" {
		inst.Scope = scope
	}
	inst.ExpiresAt = resp.expiresAt(now)
	inst.UpdatedAt = now
	if err := t.store.put(inst.OrganizationID, inst); err != nil {
		return "", err
	}
	return inst.AccessToken, nil
}

func (t *tokens) installation(organizationID string) (installation, error) {
	if organizationID != "" {
		inst, ok := t.store.get(organizationID)
		if !ok {
			return installation{}, fmt.Errorf("linear app is not installed in workspace %s; visit %s", organizationID, InstallPath)
		}
		return inst, nil
	}
	all := t.store.all()
	switch len(all) {
	case 0:
		return installation{}, fmt.Errorf("linear app is not installed yet; visit %s", InstallPath)
	case 1:
		for _, inst := range all {
			return inst, nil
		}
	}
	return installation{}, errors.New("event names no workspace and several linear installations exist")
}

// exchange posts one grant to the token endpoint.
func (t *tokens) exchange(ctx context.Context, form url.Values) (tokenResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoints.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return tokenResponse{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := t.httpClient.Do(req)
	if err != nil {
		return tokenResponse{}, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return tokenResponse{}, err
	}
	var out tokenResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return tokenResponse{}, fmt.Errorf("token endpoint returned HTTP %d with a non-JSON body", res.StatusCode)
	}
	if res.StatusCode != http.StatusOK || out.AccessToken == "" {
		reason := out.ErrorDescription
		if reason == "" {
			reason = out.Error
		}
		if reason == "" {
			reason = fmt.Sprintf("HTTP %d", res.StatusCode)
		}
		return tokenResponse{}, fmt.Errorf("token endpoint: %s", reason)
	}
	return out, nil
}

// oauthStates holds install flows in progress. They live in memory: an
// install that straddles a restart just has to be started again.
type oauthStates struct {
	mu     sync.Mutex
	states map[string]time.Time // state -> expiry
}

func (s *oauthStates) issue(now time.Time) (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	state := base64.RawURLEncoding.EncodeToString(buf)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	s.states[state] = now.Add(stateTTL)
	return state, nil
}

// consume reports whether state is live, and burns it.
func (s *oauthStates) consume(state string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	if _, ok := s.states[state]; !ok {
		return false
	}
	delete(s.states, state)
	return true
}

func (s *oauthStates) pruneLocked(now time.Time) {
	for state, expiry := range s.states {
		if !now.Before(expiry) {
			delete(s.states, state)
		}
	}
}

// handleInstall starts the OAuth install. It is gated by the install secret,
// given as ?install_secret= (for a browser) or a bearer token.
func (i *Integration) handleInstall(w http.ResponseWriter, r *http.Request) {
	provided := r.URL.Query().Get("install_secret")
	if auth := strings.TrimSpace(r.Header.Get("Authorization")); strings.HasPrefix(auth, "Bearer ") {
		provided = strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	}
	if subtle.ConstantTimeCompare([]byte(provided), []byte(i.cfg.InstallSecret)) != 1 {
		http.Error(w, "missing or invalid install secret", http.StatusUnauthorized)
		return
	}

	state, err := i.states.issue(i.now())
	if err != nil {
		i.logger.Error("issue linear oauth state", slog.Any("error", err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	u, err := url.Parse(i.endpoints.AuthorizeURL)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	q := u.Query()
	q.Set("client_id", i.cfg.ClientID)
	q.Set("redirect_uri", i.cfg.redirectURI())
	q.Set("response_type", "code")
	q.Set("scope", oauthScopes)
	q.Set("state", state)
	q.Set("actor", "app")
	u.RawQuery = q.Encode()
	http.Redirect(w, r, u.String(), http.StatusFound)
}

// handleCallback completes the install: it trades the code for a token and
// stores it under the workspace it belongs to.
func (i *Integration) handleCallback(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if e := q.Get("error"); e != "" {
		http.Error(w, "linear oauth error: "+e, http.StatusBadRequest)
		return
	}
	code, state := q.Get("code"), q.Get("state")
	if code == "" || state == "" {
		http.Error(w, "missing oauth code or state", http.StatusBadRequest)
		return
	}
	if !i.states.consume(state, i.now()) {
		http.Error(w, "invalid or expired oauth state; start again from "+InstallPath, http.StatusUnauthorized)
		return
	}

	ctx := r.Context()
	tok, err := i.tokens.exchange(ctx, url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {i.cfg.redirectURI()},
		"client_id":     {i.cfg.ClientID},
		"client_secret": {i.cfg.ClientSecret},
	})
	if err != nil {
		i.logger.Error("linear oauth code exchange failed", slog.Any("error", err))
		http.Error(w, "linear token exchange failed", http.StatusBadGateway)
		return
	}

	var who struct {
		Viewer struct {
			ID string `json:"id"`
		} `json:"viewer"`
		Organization struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"organization"`
	}
	if err := i.client.queryWithToken(ctx, tok.AccessToken, `query { viewer { id } organization { id name } }`, nil, &who); err != nil {
		i.logger.Error("linear viewer query failed", slog.Any("error", err))
		http.Error(w, "linear viewer query failed", http.StatusBadGateway)
		return
	}
	if who.Organization.ID == "" {
		http.Error(w, "linear did not report a workspace for this install", http.StatusBadGateway)
		return
	}

	now := i.now()
	inst := installation{
		OrganizationID:   who.Organization.ID,
		OrganizationName: who.Organization.Name,
		AppUserID:        who.Viewer.ID,
		AccessToken:      tok.AccessToken,
		RefreshToken:     tok.RefreshToken,
		Scope:            tok.scopeString(),
		ExpiresAt:        tok.expiresAt(now),
		InstalledAt:      now,
		UpdatedAt:        now,
	}
	if prev, ok := i.tokens.store.get(inst.OrganizationID); ok {
		inst.InstalledAt = prev.InstalledAt
	}
	if err := i.tokens.store.put(inst.OrganizationID, inst); err != nil {
		i.logger.Error("store linear installation", slog.Any("error", err))
		http.Error(w, "failed to store the installation", http.StatusInternalServerError)
		return
	}

	i.logger.Info("linear app installed",
		slog.String("organization_id", inst.OrganizationID),
		slog.String("organization", inst.OrganizationName),
		slog.String("app_user_id", inst.AppUserID),
		slog.String("scope", inst.Scope),
	)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = fmt.Fprintf(w, "ButterBox is installed in the Linear workspace %q. You can close this tab.\n", inst.OrganizationName)
}

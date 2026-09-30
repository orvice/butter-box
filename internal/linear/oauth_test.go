package linear

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func serve(i *Integration, req *http.Request) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	i.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

// startInstall runs /linear/install with the secret and returns the Linear
// authorize redirect.
func startInstall(t *testing.T, i *Integration) *url.URL {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, InstallPath+"?install_secret="+url.QueryEscape(i.cfg.InstallSecret), nil)
	rec := serve(i, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("install status = %d (%s)", rec.Code, rec.Body)
	}
	loc, err := url.Parse(rec.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

func TestInstallRequiresSecret(t *testing.T) {
	i := newTestIntegration(t, testConfig(t), newFakeAgent(), nil)

	for name, req := range map[string]*http.Request{
		"no secret":    httptest.NewRequest(http.MethodGet, InstallPath, nil),
		"wrong secret": httptest.NewRequest(http.MethodGet, InstallPath+"?install_secret=wrong", nil),
	} {
		if rec := serve(i, req); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s: status = %d, want 401", name, rec.Code)
		}
	}

	bearer := httptest.NewRequest(http.MethodGet, InstallPath, nil)
	bearer.Header.Set("Authorization", "Bearer "+i.cfg.InstallSecret)
	if rec := serve(i, bearer); rec.Code != http.StatusFound {
		t.Errorf("bearer secret: status = %d, want 302", rec.Code)
	}

	loc := startInstall(t, i)
	q := loc.Query()
	if loc.Host != "linear.invalid" || loc.Path != "/oauth/authorize" {
		t.Fatalf("redirect = %s", loc)
	}
	want := map[string]string{
		"client_id":     "client-id",
		"redirect_uri":  "https://box.example.com/linear/oauth/callback",
		"response_type": "code",
		"scope":         "read,write,app:assignable,app:mentionable",
		"actor":         "app",
	}
	for k, v := range want {
		if q.Get(k) != v {
			t.Errorf("%s = %q, want %q", k, q.Get(k), v)
		}
	}
	if q.Get("state") == "" {
		t.Error("state is missing")
	}
}

func TestOAuthCallbackStoresInstallation(t *testing.T) {
	fl := newFakeLinear(t)
	cfg := testConfig(t)
	i := newTestIntegration(t, cfg, newFakeAgent(), fl)

	state := startInstall(t, i).Query().Get("state")
	callback := CallbackPath + "?code=auth-code&state=" + url.QueryEscape(state)
	rec := serve(i, httptest.NewRequest(http.MethodGet, callback, nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Acme") {
		t.Fatalf("callback status = %d (%s)", rec.Code, rec.Body)
	}

	grants, _ := fl.snapshot()
	if len(grants) != 1 {
		t.Fatalf("grants = %v", grants)
	}
	g := grants[0]
	if g.Get("grant_type") != "authorization_code" || g.Get("code") != "auth-code" || g.Get("redirect_uri") != "https://box.example.com/linear/oauth/callback" {
		t.Fatalf("grant = %v", g)
	}

	inst, ok := i.tokens.store.get("org-1")
	if !ok || inst.AccessToken != "access-1" || inst.RefreshToken != "refresh-1" || inst.AppUserID != "app-user-1" || inst.OrganizationName != "Acme" {
		t.Fatalf("installation = %+v", inst)
	}
	if inst.ExpiresAt.IsZero() {
		t.Fatal("expiry was not recorded")
	}
	info, err := os.Stat(filepath.Join(cfg.StateDir, "installations.json"))
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Fatalf("installations.json mode = %o, want 600", mode)
	}

	// A state works once.
	if rec := serve(i, httptest.NewRequest(http.MethodGet, callback, nil)); rec.Code != http.StatusUnauthorized {
		t.Fatalf("replayed state: status = %d, want 401", rec.Code)
	}
}

func TestOAuthCallbackRejectsUnknownState(t *testing.T) {
	fl := newFakeLinear(t)
	i := newTestIntegration(t, testConfig(t), newFakeAgent(), fl)

	rec := serve(i, httptest.NewRequest(http.MethodGet, CallbackPath+"?code=c&state=forged", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if grants, _ := fl.snapshot(); len(grants) != 0 {
		t.Fatalf("forged state reached the token endpoint: %v", grants)
	}
}

func TestActivityPostRefreshesExpiredToken(t *testing.T) {
	fl := newFakeLinear(t)
	i := newTestIntegration(t, testConfig(t), newFakeAgent(), fl)
	now := time.Now()
	if err := i.tokens.store.put("org-1", installation{
		OrganizationID: "org-1",
		AccessToken:    "old-access",
		RefreshToken:   "old-refresh",
		ExpiresAt:      now.Add(time.Minute), // inside the refresh skew
		InstalledAt:    now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatal(err)
	}

	if err := i.client.createActivity(context.Background(), "org-1", "ls-1", thought("hello")); err != nil {
		t.Fatalf("createActivity: %v", err)
	}

	grants, activities := fl.snapshot()
	if len(grants) != 1 || grants[0].Get("grant_type") != "refresh_token" || grants[0].Get("refresh_token") != "old-refresh" {
		t.Fatalf("grants = %v", grants)
	}
	if len(activities) != 1 || activities[0].Token != "access-1" {
		t.Fatalf("activities = %+v", activities)
	}
	input := activities[0].Input
	content, _ := input["content"].(map[string]any)
	if input["agentSessionId"] != "ls-1" || content["type"] != "thought" || content["body"] != "hello" {
		t.Fatalf("activity input = %v", input)
	}
	if _, set := input["ephemeral"]; set {
		t.Fatalf("non-ephemeral activity sent ephemeral: %v", input)
	}

	inst, _ := i.tokens.store.get("org-1")
	if inst.AccessToken != "access-1" || inst.RefreshToken != "refresh-1" {
		t.Fatalf("installation after refresh = %+v", inst)
	}

	// A fresh token is reused without another grant.
	if err := i.client.createActivity(context.Background(), "org-1", "ls-1", thought("again")); err != nil {
		t.Fatal(err)
	}
	if grants, _ := fl.snapshot(); len(grants) != 1 {
		t.Fatalf("fresh token was refreshed again: %v", grants)
	}
}

func TestAccessTokenSelectsWorkspace(t *testing.T) {
	i := newTestIntegration(t, testConfig(t), newFakeAgent(), nil)
	ctx := context.Background()

	if _, err := i.tokens.accessToken(ctx, ""); err == nil || !strings.Contains(err.Error(), "not installed") {
		t.Fatalf("no installs: err = %v", err)
	}

	now := time.Now()
	for _, org := range []string{"org-1", "org-2"} {
		if err := i.tokens.store.put(org, installation{OrganizationID: org, AccessToken: "token-" + org, InstalledAt: now, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	if tok, err := i.tokens.accessToken(ctx, "org-2"); err != nil || tok != "token-org-2" {
		t.Fatalf("org-2: token = %q, err = %v", tok, err)
	}
	if _, err := i.tokens.accessToken(ctx, ""); err == nil {
		t.Fatal("ambiguous workspace was accepted")
	}
	if _, err := i.tokens.accessToken(ctx, "org-3"); err == nil || !strings.Contains(err.Error(), "org-3") {
		t.Fatalf("unknown workspace: err = %v", err)
	}
}

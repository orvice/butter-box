package pi

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Opt-in: exercises the installed Pi, not fakePi, against an isolated local
// OpenAI-compatible endpoint. No developer sessions, settings, or credentials
// are touched, and no paid provider requests are made.
func TestRealPiModelSelectionSmoke(t *testing.T) {
	if os.Getenv("BUTTERBOX_PI_SMOKE") != "1" {
		t.Skip("set BUTTERBOX_PI_SMOKE=1 to test the installed Pi runtime")
	}
	bin, err := exec.LookPath("pi")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Second)
	defer cancel()
	type providerRequest struct {
		path       string
		hasHistory bool
	}
	requests := make(chan providerRequest, 16)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.NotFound(w, r)
			return
		}
		var request struct {
			Model    string `json:"model"`
			Messages []struct {
				Role    string `json:"role"`
				Content any    `json:"content"`
			} `json:"messages"`
		}
		if err := json.UnmarshalRead(r.Body, &request); err != nil {
			t.Errorf("decode provider request: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if request.Model != "shared-model" {
			t.Errorf("provider requested model = %q", request.Model)
		}
		hasHistory := false
		for _, message := range request.Messages {
			if message.Role == "assistant" {
				hasHistory = true
			}
		}
		requests <- providerRequest{path: r.URL.Path, hasHistory: hasHistory}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: "+`{"id":"smoke","object":"chat.completion.chunk","created":1,"model":"shared-model","choices":[{"index":0,"delta":{"role":"assistant","content":"smoke reply"},"finish_reason":null}]}`+"\n\n")
		_, _ = fmt.Fprint(w, "data: "+`{"id":"smoke","object":"chat.completion.chunk","created":1,"model":"shared-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}`+"\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(endpoint.Close)

	agentDir, cwd, sessionDir := t.TempDir(), t.TempDir(), t.TempDir()
	t.Setenv("PI_CODING_AGENT_DIR", agentDir)
	t.Setenv("PI_OFFLINE", "1")
	t.Setenv("PI_TELEMETRY", "0")
	t.Setenv("PI_SKIP_VERSION_CHECK", "1")
	providers := map[string]any{}
	for _, provider := range []string{"smoke-a", "smoke-b"} {
		providers[provider] = map[string]any{
			"baseUrl": endpoint.URL + "/" + provider + "/v1", "api": "openai-completions", "apiKey": "smoke-only",
			"models": []map[string]any{{
				"id": "shared-model", "name": provider, "reasoning": provider == "smoke-a",
				"input": []string{"text"}, "contextWindow": 128000, "maxTokens": 1024,
				"cost": map[string]any{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
			}},
		}
	}
	models, err := json.Marshal(map[string]any{"providers": providers})
	if err != nil {
		t.Fatal(err)
	}
	settings := []byte(`{"defaultProvider":"smoke-a","defaultModel":"shared-model","defaultThinkingLevel":"high"}`)
	for name, data := range map[string][]byte{"models.json": models, "settings.json": settings} {
		if err := os.WriteFile(filepath.Join(agentDir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manager := NewManager(testLogger(t), Config{Bin: bin, SessionDir: sessionDir, SandboxRoot: cwd, IdleTimeout: 100 * time.Millisecond})
	t.Cleanup(manager.Stop)
	original, err := manager.Create(ctx, CreateOpts{Provider: "smoke-a", Model: "shared-model", Cwd: cwd})
	if err != nil {
		t.Fatal(err)
	}
	var previous []EntryRecord
	submit := func(provider string, selection *ModelSelection, thinking string) {
		t.Helper()
		wantHistory := len(previous) > 0
		result, err := manager.SubmitWithModel(ctx, original.ID, "Reply briefly without using tools.", nil, selection)
		if err != nil {
			t.Fatal(err)
		}
		if result.AppliedModel == nil || result.AppliedModel.Provider != provider || result.AppliedModel.ModelID != "shared-model" {
			t.Fatalf("applied model = %+v", result.AppliedModel)
		}
		turn, err := manager.Turn(ctx, original.ID, result.Cursor, 10*time.Second)
		if err != nil || turn.Running || turn.Result == nil || turn.Result.Text != "smoke reply" || turn.Result.StopReason != "stop" {
			t.Fatalf("turn = %+v, error = %v", turn, err)
		}
		info, _, err := manager.Get(ctx, original.ID)
		if err != nil || info.ID != original.ID || info.File != original.File || info.Cwd != original.Cwd || info.Model != provider+"/shared-model" {
			t.Fatalf("get_state continuity = %+v, error = %v", info, err)
		}
		entries, err := manager.Entries(ctx, original.ID, "")
		if err != nil {
			t.Fatal(err)
		}
		if len(entries.Entries) <= len(previous) {
			t.Fatal("transcript did not grow")
		}
		for i, entry := range previous {
			if string(entry.Raw) != string(entries.Entries[i].Raw) {
				t.Fatalf("history changed at %s", entry.ID)
			}
		}
		var lastModel, assistantProvider, assistantModel, lastThinking, assistantThinking string
		for _, entry := range entries.Entries {
			var payload struct {
				Provider      string `json:"provider"`
				ModelID       string `json:"modelId"`
				ThinkingLevel string `json:"thinkingLevel"`
				Message       struct {
					Role          string `json:"role"`
					Provider      string `json:"provider"`
					Model         string `json:"model"`
					ThinkingLevel string `json:"thinkingLevel"`
				} `json:"message"`
			}
			if err := json.Unmarshal(entry.Raw, &payload); err != nil {
				t.Fatal(err)
			}
			if entry.Type == "model_change" {
				lastModel = payload.Provider + "/" + payload.ModelID
			}
			if entry.Type == "thinking_level_change" {
				lastThinking = payload.ThinkingLevel
			}
			if payload.Message.Role == "assistant" {
				assistantProvider, assistantModel, assistantThinking = payload.Message.Provider, payload.Message.Model, payload.Message.ThinkingLevel
			}
		}
		if lastModel != provider+"/shared-model" || assistantProvider != provider || assistantModel != "shared-model" || lastThinking != thinking || assistantThinking != thinking {
			t.Fatalf("model_change=%s assistant=%s/%s thinking=%s/%s, want %s/shared-model %s", lastModel, assistantProvider, assistantModel, lastThinking, assistantThinking, provider, thinking)
		}
		previous = entries.Entries
		select {
		case request := <-requests:
			if request.path != "/"+provider+"/v1/chat/completions" || request.hasHistory != wantHistory {
				t.Fatalf("physical provider request = %+v, want %s with history=%t", request, provider, wantHistory)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	submit("smoke-a", &ModelSelection{Provider: "smoke-a", ModelID: "shared-model"}, "high")
	submit("smoke-b", &ModelSelection{Provider: "smoke-b", ModelID: "shared-model"}, "off")
	submit("smoke-b", nil, "off")
	// Exercise the real idle janitor (one-minute tick), not Delete/purge.
	for {
		infos, err := manager.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(infos) == 1 && !infos[0].Active {
			break
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	resumed, _, err := manager.Get(ctx, original.ID)
	if err != nil || resumed.Model != "smoke-b/shared-model" || resumed.File != original.File || resumed.Cwd != cwd {
		t.Fatalf("idle re-attach = %+v, error = %v", resumed, err)
	}
	submit("smoke-a", &ModelSelection{Provider: "smoke-a", ModelID: "shared-model"}, "high")
	for name, want := range map[string][]byte{"models.json": models, "settings.json": settings} {
		got, err := os.ReadFile(filepath.Join(agentDir, name))
		if err != nil || string(got) != string(want) {
			t.Fatalf("global config %s changed: %v", name, err)
		}
	}
}

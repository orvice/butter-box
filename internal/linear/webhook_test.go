package linear

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func webhookBody(t *testing.T, typ, action string, ts time.Time, extra map[string]any) []byte {
	t.Helper()
	payload := map[string]any{
		"type":             typ,
		"action":           action,
		"organizationId":   "org-1",
		"webhookTimestamp": ts.UnixMilli(),
		"agentSession": map[string]any{
			"id":    "ls-1",
			"issue": map[string]any{"identifier": "ENG-1", "title": "Fix login"},
		},
	}
	for k, v := range extra {
		payload[k] = v
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// captureDispatch replaces the runner with a channel of dispatched events.
func captureDispatch(i *Integration) chan sessionEvent {
	ch := make(chan sessionEvent, 8)
	i.dispatch = func(ev sessionEvent) { ch <- ev }
	return ch
}

func postWebhook(t *testing.T, i *Integration, body []byte, signature, delivery string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	i.Register(mux)
	req := httptest.NewRequest(http.MethodPost, WebhookPath, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if signature != "" {
		req.Header.Set("Linear-Signature", signature)
	}
	if delivery != "" {
		req.Header.Set("Linear-Delivery", delivery)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestValidSignature(t *testing.T) {
	body := []byte(`{"type":"AgentSessionEvent"}`)
	good := sign("secret", body)
	tests := []struct {
		name   string
		header string
		want   bool
	}{
		{"matching signature", good, true},
		{"surrounding whitespace", "  " + good + " ", true},
		{"missing", "", false},
		{"not hex", "zz-not-hex", false},
		{"other secret", sign("other", body), false},
		{"truncated", good[:32], false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validSignature("secret", tt.header, body); got != tt.want {
				t.Fatalf("validSignature = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWebhookDispatchesAgentSessionEvents(t *testing.T) {
	i := newTestIntegration(t, testConfig(t), newFakeAgent(), nil)
	events := captureDispatch(i)

	body := webhookBody(t, "AgentSessionEvent", "prompted", time.Now(), map[string]any{
		"agentActivity": map[string]any{"signal": "stop", "content": map[string]any{"type": "prompt", "body": "stop please"}},
	})
	rec := postWebhook(t, i, body, sign("webhook-secret", body), "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"accepted":true`) {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}

	select {
	case ev := <-events:
		if ev.AgentSession.ID != "ls-1" || ev.OrganizationID != "org-1" || !ev.isStop() || ev.message() != "stop please" {
			t.Fatalf("event = %+v", ev)
		}
		if ev.issueLabel() != "ENG-1: Fix login" {
			t.Fatalf("issue label = %q", ev.issueLabel())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("event was not dispatched")
	}
}

func TestWebhookRejections(t *testing.T) {
	now := time.Now()
	fresh := webhookBody(t, "AgentSessionEvent", "created", now, nil)
	stale := webhookBody(t, "AgentSessionEvent", "created", now.Add(-2*time.Minute), nil)
	future := webhookBody(t, "AgentSessionEvent", "created", now.Add(2*time.Minute), nil)

	tests := []struct {
		name      string
		body      []byte
		signature string
		want      int
	}{
		{"unsigned", fresh, "", http.StatusUnauthorized},
		{"wrong secret", fresh, sign("nope", fresh), http.StatusUnauthorized},
		{"signature over another body", fresh, sign("webhook-secret", stale), http.StatusUnauthorized},
		{"stale timestamp", stale, sign("webhook-secret", stale), http.StatusUnauthorized},
		{"future timestamp", future, sign("webhook-secret", future), http.StatusUnauthorized},
		{"invalid json", []byte("{"), sign("webhook-secret", []byte("{")), http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := newTestIntegration(t, testConfig(t), newFakeAgent(), nil)
			events := captureDispatch(i)
			rec := postWebhook(t, i, tt.body, tt.signature, "")
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tt.want, rec.Body)
			}
			select {
			case ev := <-events:
				t.Fatalf("rejected delivery was dispatched: %+v", ev)
			default:
			}
		})
	}
}

func TestWebhookIgnoresOtherEventTypes(t *testing.T) {
	i := newTestIntegration(t, testConfig(t), newFakeAgent(), nil)
	events := captureDispatch(i)

	body := webhookBody(t, "Issue", "update", time.Now(), nil)
	rec := postWebhook(t, i, body, sign("webhook-secret", body), "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"accepted":false`) {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	select {
	case ev := <-events:
		t.Fatalf("non-session event was dispatched: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWebhookDeduplicatesDeliveries(t *testing.T) {
	i := newTestIntegration(t, testConfig(t), newFakeAgent(), nil)
	events := captureDispatch(i)

	body := webhookBody(t, "AgentSessionEvent", "created", time.Now(), nil)
	for n := range 2 {
		rec := postWebhook(t, i, body, sign("webhook-secret", body), "delivery-1")
		if rec.Code != http.StatusOK {
			t.Fatalf("delivery %d: status = %d", n, rec.Code)
		}
	}
	<-events
	select {
	case ev := <-events:
		t.Fatalf("redelivery was dispatched: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWebhookBodyLimit(t *testing.T) {
	i := newTestIntegration(t, testConfig(t), newFakeAgent(), nil)
	captureDispatch(i)
	body := []byte(fmt.Sprintf(`{"pad":%q}`, strings.Repeat("x", maxWebhookBytes)))
	rec := postWebhook(t, i, body, sign("webhook-secret", body), "")
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", rec.Code)
	}
}

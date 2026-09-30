package linear

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	maxWebhookBytes = 1 << 20
	// webhookMaxSkew rejects replayed deliveries: Linear stamps each one.
	webhookMaxSkew = time.Minute
	// deliveryTTL is how long a delivery ID is remembered for deduplication.
	deliveryTTL = 10 * time.Minute
)

// sessionEvent is the part of an AgentSessionEvent webhook we consume.
type sessionEvent struct {
	Type             string `json:"type"`
	Action           string `json:"action"`
	OrganizationID   string `json:"organizationId"`
	WebhookTimestamp int64  `json:"webhookTimestamp"` // unix ms
	// PromptContext is Linear's formatted issue, comment and guidance
	// context for the session.
	PromptContext string `json:"promptContext"`
	AgentSession  struct {
		ID    string    `json:"id"`
		Issue *issueRef `json:"issue"`
	} `json:"agentSession"`
	// AgentActivity is the user's prompt on a prompted event.
	AgentActivity *promptActivity `json:"agentActivity"`
}

type issueRef struct {
	Identifier  string `json:"identifier"`
	Title       string `json:"title"`
	URL         string `json:"url"`
	Description string `json:"description"`
}

type promptActivity struct {
	Signal  string `json:"signal"`
	Content struct {
		Body string `json:"body"`
	} `json:"content"`
}

// isStop reports a user's stop request, which Linear sends as a prompt
// activity carrying the stop signal.
func (ev sessionEvent) isStop() bool {
	return ev.AgentActivity != nil && ev.AgentActivity.Signal == "stop"
}

// message is the user's text on a prompted event.
func (ev sessionEvent) message() string {
	if ev.AgentActivity == nil {
		return ""
	}
	return strings.TrimSpace(ev.AgentActivity.Content.Body)
}

// issueLabel names the session's issue for humans, e.g. "ENG-123: Fix login".
func (ev sessionEvent) issueLabel() string {
	issue := ev.AgentSession.Issue
	if issue == nil || issue.Identifier == "" {
		return ""
	}
	if issue.Title == "" {
		return issue.Identifier
	}
	return issue.Identifier + ": " + issue.Title
}

// validSignature checks the Linear-Signature header: the hex HMAC-SHA256 of
// the raw body under the webhook signing secret.
func validSignature(secret, header string, body []byte) bool {
	provided, err := hex.DecodeString(strings.TrimSpace(header))
	if err != nil || len(provided) == 0 {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hmac.Equal(provided, mac.Sum(nil))
}

func freshTimestamp(unixMs int64, now time.Time) bool {
	if unixMs <= 0 {
		return false
	}
	skew := now.Sub(time.UnixMilli(unixMs))
	return skew <= webhookMaxSkew && skew >= -webhookMaxSkew
}

// deliveries remembers recent Linear-Delivery IDs so a redelivered webhook
// does not start a second run.
type deliveries struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

// first reports whether id is new, and records it.
func (d *deliveries) first(id string, now time.Time) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for k, at := range d.seen {
		if now.Sub(at) > deliveryTTL {
			delete(d.seen, k)
		}
	}
	if _, ok := d.seen[id]; ok {
		return false
	}
	d.seen[id] = now
	return true
}

// handleWebhook authenticates a Linear delivery and hands agent session
// events to the runner. It answers at once: Linear expects a reply within
// seconds, so all work happens after the response.
func (i *Integration) handleWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	if !validSignature(i.cfg.WebhookSecret, r.Header.Get("Linear-Signature"), body) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	var ev sessionEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	now := i.now()
	if !freshTimestamp(ev.WebhookTimestamp, now) {
		http.Error(w, "stale webhook", http.StatusUnauthorized)
		return
	}

	accepted := ev.Type == "AgentSessionEvent"
	if id := r.Header.Get("Linear-Delivery"); accepted && id != "" && !i.deliveries.first(id, now) {
		i.logger.Info("duplicate linear delivery ignored", slog.String("delivery_id", id))
		accepted = false
	}
	if accepted {
		i.logger.Info("linear agent session event",
			slog.String("action", ev.Action),
			slog.String("agent_session_id", ev.AgentSession.ID),
			slog.String("issue", ev.issueLabel()),
			slog.Bool("stop", ev.isStop()),
		)
		go i.dispatch(ev)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true, "accepted": accepted})
}

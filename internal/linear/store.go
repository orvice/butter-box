package linear

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// installation is one Linear workspace's OAuth grant for the app actor.
type installation struct {
	OrganizationID   string    `json:"organization_id"`
	OrganizationName string    `json:"organization_name,omitempty"`
	AppUserID        string    `json:"app_user_id,omitempty"`
	AccessToken      string    `json:"access_token"`
	RefreshToken     string    `json:"refresh_token,omitempty"`
	Scope            string    `json:"scope,omitempty"`
	ExpiresAt        time.Time `json:"expires_at,omitzero"` // zero: never expires
	InstalledAt      time.Time `json:"installed_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// sessionRecord maps one Linear agent session to the pi session backing it.
type sessionRecord struct {
	PiSessionID    string `json:"pi_session_id"`
	OrganizationID string `json:"organization_id,omitempty"`
	Issue          string `json:"issue,omitempty"`
	// Running is set while a run is in flight, so a run cut short by a box
	// restart can be reported on the next start.
	Running   bool      `json:"running,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// fileStore is a string-keyed map persisted as one private JSON file. Every
// mutation rewrites the file atomically before it becomes visible.
type fileStore[T any] struct {
	path string

	mu      sync.Mutex
	records map[string]T
}

func openFileStore[T any](path string) (*fileStore[T], error) {
	s := &fileStore[T]{path: path, records: map[string]T{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(data, &s.records); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	if s.records == nil {
		s.records = map[string]T{}
	}
	return s, nil
}

func (s *fileStore[T]) get(key string) (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.records[key]
	return v, ok
}

// all returns a snapshot of every record.
func (s *fileStore[T]) all() map[string]T {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.records)
}

func (s *fileStore[T]) put(key string, v T) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.cloneLocked()
	next[key] = v
	return s.commitLocked(next)
}

// update applies fn to the record under key; a missing key is a no-op.
func (s *fileStore[T]) update(key string, fn func(*T)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.records[key]
	if !ok {
		return nil
	}
	fn(&v)
	next := s.cloneLocked()
	next[key] = v
	return s.commitLocked(next)
}

func (s *fileStore[T]) delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[key]; !ok {
		return nil
	}
	next := s.cloneLocked()
	delete(next, key)
	return s.commitLocked(next)
}

func (s *fileStore[T]) cloneLocked() map[string]T {
	return maps.Clone(s.records)
}

// commitLocked persists next and only then publishes it, so memory never
// runs ahead of what a restart would read back.
func (s *fileStore[T]) commitLocked(next map[string]T) error {
	if err := writePrivateJSON(s.path, next); err != nil {
		return err
	}
	s.records = next
	return nil
}

// writePrivateJSON atomically replaces path with v encoded as JSON, readable
// by the owner only: the files hold OAuth tokens.
func writePrivateJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	defer os.Remove(tmp.Name()) // no-op once renamed
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

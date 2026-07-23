package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

var ValidActions = map[string]struct{}{
	"login":    {},
	"view":     {},
	"purchase": {},
}

type Event struct {
	EventID    uuid.UUID       `json:"event_id"`
	UserID     string          `json:"user_id"`
	Action     string          `json:"action"`
	ResourceID string          `json:"resource_id"`
	Meta       json.RawMessage `json:"meta"`
	Timestamp  time.Time       `json:"timestamp"`
}

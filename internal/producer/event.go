package producer

import "time"

type UserActionEvent struct {
	EventID    string                 `json:"event_id"`
	UserID     string                 `json:"user_id"`
	Action     string                 `json:"action"`
	ResourceID string                 `json:"resource_id"`
	Meta       map[string]interface{} `json:"meta,omitempty"`
	Timestamp  time.Time              `json:"timestamp"`
}

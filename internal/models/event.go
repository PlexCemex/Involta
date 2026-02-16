package models

import "encoding/json"

type Event struct {
	EventID   string          `json:"event_id"`
	UserID    int             `json:"user_id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

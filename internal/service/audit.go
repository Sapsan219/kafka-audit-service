package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"audit-service/internal/domain"

	"github.com/google/uuid"
)

var ErrValidation = errors.New("validation error")

type EventRepository interface {
	SaveEvent(context.Context, domain.Event) error
	DeleteEvent(context.Context, string) error
}

type EventProducer interface {
	SendEvent(context.Context, domain.Event) error
}

type AuditService struct {
	repository EventRepository
	producer   EventProducer
}

func NewAuditService(repository EventRepository, producer EventProducer) *AuditService {
	return &AuditService{repository: repository, producer: producer}
}

func (s *AuditService) CreateEvent(
	ctx context.Context,
	userID string,
	action string,
	resourceID string,
	meta json.RawMessage,
) (domain.Event, error) {
	userID = strings.TrimSpace(userID)
	action = strings.TrimSpace(action)
	resourceID = strings.TrimSpace(resourceID)

	if userID == "" || action == "" || resourceID == "" {
		return domain.Event{}, fmt.Errorf("%w: user_id, action and resource_id are required", ErrValidation)
	}
	if _, ok := domain.ValidActions[action]; !ok {
		return domain.Event{}, fmt.Errorf("%w: action must be login, view or purchase", ErrValidation)
	}
	if len(meta) == 0 || string(meta) == "null" {
		meta = json.RawMessage(`{}`)
	}

	var metaObject map[string]any
	if err := json.Unmarshal(meta, &metaObject); err != nil || metaObject == nil {
		return domain.Event{}, fmt.Errorf("%w: meta must be a JSON object", ErrValidation)
	}

	event := domain.Event{
		EventID:    uuid.New(),
		UserID:     userID,
		Action:     action,
		ResourceID: resourceID,
		Meta:       meta,
		Timestamp:  time.Now().UTC(),
	}

	if err := s.repository.SaveEvent(ctx, event); err != nil {
		return domain.Event{}, err
	}
	if err := s.producer.SendEvent(ctx, event); err != nil {
		if rollbackErr := s.repository.DeleteEvent(ctx, event.EventID.String()); rollbackErr != nil {
			return domain.Event{}, fmt.Errorf("%v; rollback database event: %w", err, rollbackErr)
		}
		return domain.Event{}, err
	}

	return event, nil
}

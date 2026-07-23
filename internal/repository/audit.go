package repository

import (
	"context"
	"fmt"

	"audit-service/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AuditRepository struct {
	pool *pgxpool.Pool
}

func NewAuditRepository(pool *pgxpool.Pool) *AuditRepository {
	return &AuditRepository{pool: pool}
}

func (r *AuditRepository) SaveEvent(ctx context.Context, event domain.Event) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO audit_log (event_id, user_id, action, resource_id, meta, timestamp)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, event.EventID, event.UserID, event.Action, event.ResourceID, event.Meta, event.Timestamp)
	if err != nil {
		return fmt.Errorf("insert audit event: %w", err)
	}
	return nil
}

func (r *AuditRepository) DeleteEvent(ctx context.Context, eventID string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM audit_log WHERE event_id = $1`, eventID)
	if err != nil {
		return fmt.Errorf("delete audit event: %w", err)
	}
	return nil
}

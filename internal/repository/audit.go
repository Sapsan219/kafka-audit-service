package repository

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type AuditRepository struct {
	pool *pgxpool.Pool
}

func NewAuditRepository(pool *pgxpool.Pool) *AuditRepository {
	return &AuditRepository{pool: pool}
}

func (r *AuditRepository) SaveEvent(ctx context.Context, eventID, userID, action, resourceID string, meta []byte, timestamp string) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO audit_log (event_id, user_id, action, resource_id, meta, timestamp)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, eventID, userID, action, resourceID, meta, timestamp)
	return err
}

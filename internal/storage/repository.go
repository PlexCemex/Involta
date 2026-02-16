package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq"
)

var ErrDuplicate = errors.New("event already processed")

type Repository struct {
	conn *sql.DB
}

func New(dsn string) (*Repository, error) {
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(); err != nil {
		return nil, err
	}
	return &Repository{conn: conn}, nil
}

func (r *Repository) Setup(ctx context.Context) error {
	_, err := r.conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS processed_events (
			event_id TEXT PRIMARY KEY,
			created_at TIMESTAMP DEFAULT NOW()
		)
	`)
	return err
}

func (r *Repository) SaveEvent(ctx context.Context, eventID string) error {
	res, err := r.conn.ExecContext(ctx,
		"INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING",
		eventID,
	)
	if err != nil {
		return fmt.Errorf("storage error: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected error: %w", err)
	}

	if rows == 0 {
		return ErrDuplicate
	}

	return nil
}

func (r *Repository) Close() error {
	return r.conn.Close()
}

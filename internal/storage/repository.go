package storage

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

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
	if err != nil {
		return err
	}

	_, err = r.conn.ExecContext(ctx, `
		ALTER TABLE processed_events 
		ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending'
	`)
	return err
}

func (r *Repository) StartProcessing(ctx context.Context, eventID string) (bool, error) {
	tx, err := r.conn.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var status string
	err = tx.QueryRowContext(ctx, "SELECT status FROM processed_events WHERE event_id = $1 FOR UPDATE", eventID).Scan(&status)

	if err == sql.ErrNoRows {
		_, err = tx.ExecContext(ctx, "INSERT INTO processed_events (event_id, status) VALUES ($1, 'pending')", eventID)
		if err != nil {
			return false, fmt.Errorf("insert error: %w", err)
		}
		return true, tx.Commit()
	} else if err != nil {
		return false, fmt.Errorf("select error: %w", err)
	}

	if status == "done" {
		return false, nil
	}

	return true, tx.Commit()
}

func (r *Repository) MarkDone(ctx context.Context, eventID string) error {
	_, err := r.conn.ExecContext(ctx, "UPDATE processed_events SET status = 'done' WHERE event_id = $1", eventID)
	return err
}

func (r *Repository) Close() error {
	return r.conn.Close()
}

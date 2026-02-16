package storage

import (
	"context"
	"database/sql"

	_ "github.com/lib/pq"
)

type DB struct {
	conn *sql.DB
}

func New(dsn string) (*DB, error) {
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return &DB{conn: conn}, conn.Ping()
}

func (d *DB) Setup() error {
	_, err := d.conn.Exec(`CREATE TABLE IF NOT EXISTS processed_events (event_id UUID PRIMARY KEY, created_at TIMESTAMP)`)
	return err
}

func (d *DB) IsDuplicate(ctx context.Context, eventID string) (bool, error) {
	var exists bool
	err := d.conn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id=$1)", eventID).Scan(&exists)
	return exists, err
}

func (d *DB) Save(ctx context.Context, eventID string) error {
	_, err := d.conn.ExecContext(ctx, "INSERT INTO processed_events (event_id, created_at) VALUES ($1, NOW()) ON CONFLICT DO NOTHING", eventID)
	return err
}

func (d *DB) Close() { d.conn.Close() }

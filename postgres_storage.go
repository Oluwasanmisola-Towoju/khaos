package khaos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	// database/sql defines DB interfaces but no PostgreSQL wire protocol.
	// The blank import registers lib/pq as the "postgres" driver so
	// sql.Open("postgres", ...) works.\\\/
	_ "github.com/lib/pq"
)

// RiderUpdate is the client payload for location pings.
// RiderID is passed as the operation key; UpdatedAt is server-assigned.
type RiderUpdate struct {
	OrderID       string  `json:"order_id"`
	Latitude      float64 `json:"latitude"`
	Longitude     float64 `json:"longitude"`
	CurrentStatus string  `json:"current_status"`
	ETAMinutes    int     `json:"eta_minutes"`
}

// RiderRecord is the read shape: RiderUpdate plus UpdatedAt.
type RiderRecord struct {
	RiderUpdate
	UpdatedAt time.Time `json:"updated_at"`
}

// validRiderStatuses mirrors the table CHECK constraint for quick local
// validation before writing to the database.
var validRiderStatuses = map[string]bool{
	"AT_VENDOR": true,
	"EN_ROUTE":  true,
	"ARRIVED":   true,
	"DELIVERED": true,
}

// PostgresStorageEngine implements StorageEngine over
// active_rider_tracking using prepared statements and bind parameters.
// *sql.DB and prepared *sql.Stmt are concurrency-safe, so no extra locks
// are required.
type PostgresStorageEngine struct {
	db         *sql.DB
	getStmt    *sql.Stmt
	setStmt    *sql.Stmt
	deleteStmt *sql.Stmt
}

const (
	getQuery = `
		SELECT order_id, latitude, longitude, current_status, eta_minutes, updated_at
		FROM active_rider_tracking
		WHERE rider_id = $1`

	// Upsert keeps one row per rider by updating existing rider_id records.
	setQuery = `
		INSERT INTO active_rider_tracking
			(rider_id, order_id, latitude, longitude, current_status, eta_minutes, updated_at)
		VALUES
			($1, $2, $3, $4, $5, $6, now())
		ON CONFLICT (rider_id) DO UPDATE SET
			order_id       = EXCLUDED.order_id,
			latitude       = EXCLUDED.latitude,
			longitude      = EXCLUDED.longitude,
			current_status = EXCLUDED.current_status,
			eta_minutes    = EXCLUDED.eta_minutes,
			updated_at     = EXCLUDED.updated_at`

	deleteQuery = `DELETE FROM active_rider_tracking WHERE rider_id = $1`
)

// NewPostgresStorageEngine prepares statements once and returns a
// ready-to-use engine. The caller owns DB configuration and lifecycle.
func NewPostgresStorageEngine(ctx context.Context, db *sql.DB) (*PostgresStorageEngine, error) {
	getStmt, err := db.PrepareContext(ctx, getQuery)
	if err != nil {
		return nil, fmt.Errorf("khaos: preparing get statement: %w", err)
	}

	setStmt, err := db.PrepareContext(ctx, setQuery)
	if err != nil {
		getStmt.Close()
		return nil, fmt.Errorf("khaos: preparing set statement: %w", err)
	}

	deleteStmt, err := db.PrepareContext(ctx, deleteQuery)
	if err != nil {
		getStmt.Close()
		setStmt.Close()
		return nil, fmt.Errorf("khaos: preparing delete statement: %w", err)
	}

	return &PostgresStorageEngine{
		db:         db,
		getStmt:    getStmt,
		setStmt:    setStmt,
		deleteStmt: deleteStmt,
	}, nil
}

// Get returns rider_id data. Missing rows return (nil, false, nil).
func (p *PostgresStorageEngine) Get(ctx context.Context, key string) (any, bool, error) {
	var rec RiderRecord
	row := p.getStmt.QueryRowContext(ctx, key)
	err := row.Scan(
		&rec.OrderID,
		&rec.Latitude,
		&rec.Longitude,
		&rec.CurrentStatus,
		&rec.ETAMinutes,
		&rec.UpdatedAt,
	)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("khaos: get rider %s: %w", key, err)
	}
	return rec, true, nil
}

// Set writes rider_id data. value must be RiderUpdate or *RiderUpdate.
// Type or validation failures are returned as errors.
func (p *PostgresStorageEngine) Set(ctx context.Context, key string, value any) error {
	update, err := asRiderUpdate(value)
	if err != nil {
		return fmt.Errorf("khaos: set rider %s: %w", key, err)
	}
	if !validRiderStatuses[update.CurrentStatus] {
		return fmt.Errorf("khaos: set rider %s: invalid current_status %q", key, update.CurrentStatus)
	}
	if update.ETAMinutes < 0 {
		return fmt.Errorf("khaos: set rider %s: eta_minutes must be >= 0, got %d", key, update.ETAMinutes)
	}

	_, err = p.setStmt.ExecContext(ctx,
		key,
		update.OrderID,
		update.Latitude,
		update.Longitude,
		update.CurrentStatus,
		update.ETAMinutes,
	)
	if err != nil {
		return fmt.Errorf("khaos: set rider %s: %w", key, err)
	}
	return nil
}

// Delete removes rider_id data. Missing rows are treated as a no-op.
func (p *PostgresStorageEngine) Delete(ctx context.Context, key string) error {
	if _, err := p.deleteStmt.ExecContext(ctx, key); err != nil {
		return fmt.Errorf("khaos: delete rider %s: %w", key, err)
	}
	return nil
}

// Close releases prepared statements. It does not close the caller-owned DB.
func (p *PostgresStorageEngine) Close() error {
	var errs []error
	if err := p.getStmt.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := p.setStmt.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := p.deleteStmt.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// asRiderUpdate accepts RiderUpdate in value or pointer form.
func asRiderUpdate(value any) (RiderUpdate, error) {
	switch v := value.(type) {
	case RiderUpdate:
		return v, nil
	case *RiderUpdate:
		if v == nil {
			return RiderUpdate{}, errors.New("value is a nil *RiderUpdate")
		}
		return *v, nil
	default:
		return RiderUpdate{}, fmt.Errorf("expected RiderUpdate or *RiderUpdate, got %T", value)
	}
}

// Compile-time interface check.
var _ StorageEngine = (*PostgresStorageEngine)(nil)
package storage

import (
	"context"
	"database/sql"
	"log"
	"time"

	"bankovskoe/internal/domain"

	_ "github.com/lib/pq"
)

type Store struct {
	DB *sql.DB
}

func Connect(ctx context.Context, databaseURL string) (*sql.DB, error) {
	database, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, err
	}

	database.SetMaxOpenConns(10)
	database.SetMaxIdleConns(5)
	database.SetConnMaxLifetime(30 * time.Minute)

	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		lastErr = database.PingContext(pingCtx)
		cancel()
		if lastErr == nil {
			return database, nil
		}

		log.Printf("[postgres] waiting for database, attempt=%d error=%v", attempt, lastErr)
		time.Sleep(time.Second)
	}

	database.Close()
	return nil, lastErr
}

func NewStore(db *sql.DB) *Store {
	return &Store{DB: db}
}

func (s *Store) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

func (s *Store) CreatePaymentTx(ctx context.Context, tx *sql.Tx, idempotencyKey string, amount int) (domain.Payment, error) {
	var payment domain.Payment
	err := tx.QueryRowContext(ctx, `
		INSERT INTO payments (idempotency_key, amount, status)
		VALUES ($1, $2, 'PENDING')
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id, idempotency_key, amount, status
	`, idempotencyKey, amount).Scan(&payment.ID, &payment.IdempotencyKey, &payment.Amount, &payment.Status)
	return payment, err
}

func (s *Store) GetPaymentByIdempotencyKey(ctx context.Context, idempotencyKey string) (domain.Payment, error) {
	var payment domain.Payment
	err := s.DB.QueryRowContext(ctx, `
		SELECT id, idempotency_key, amount, status
		FROM payments
		WHERE idempotency_key = $1
	`, idempotencyKey).Scan(&payment.ID, &payment.IdempotencyKey, &payment.Amount, &payment.Status)
	return payment, err
}

func (s *Store) InsertPaymentAttemptTx(ctx context.Context, tx *sql.Tx, attempt domain.PaymentAttempt) error {
	return tx.QueryRowContext(ctx, `
		INSERT INTO payment_attempts (payment_id, bank_name, attempt_num, status, http_status)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, attempt.PaymentID, attempt.BankName, attempt.AttemptNum, attempt.Status, attempt.HTTPStatus).Scan(&attempt.ID)
}

func (s *Store) UpdatePaymentStatusTx(ctx context.Context, tx *sql.Tx, paymentID int, status string) (domain.Payment, error) {
	var payment domain.Payment
	err := tx.QueryRowContext(ctx, `
		UPDATE payments
		SET status = $2, updated_at = now()
		WHERE id = $1
		RETURNING id, idempotency_key, amount, status
	`, paymentID, status).Scan(&payment.ID, &payment.IdempotencyKey, &payment.Amount, &payment.Status)
	return payment, err
}

func (s *Store) GetPaymentByID(ctx context.Context, paymentID int) (domain.Payment, error) {
	var payment domain.Payment
	err := s.DB.QueryRowContext(ctx, `
		SELECT id, idempotency_key, amount, status
		FROM payments
		WHERE id = $1
	`, paymentID).Scan(&payment.ID, &payment.IdempotencyKey, &payment.Amount, &payment.Status)
	return payment, err
}

func (s *Store) ListPaymentAttempts(ctx context.Context, paymentID int) ([]domain.PaymentAttempt, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, payment_id, bank_name, attempt_num, status, http_status
		FROM payment_attempts
		WHERE payment_id = $1
		ORDER BY attempt_num
	`, paymentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	attempts := []domain.PaymentAttempt{}
	for rows.Next() {
		var attempt domain.PaymentAttempt
		if err := rows.Scan(&attempt.ID, &attempt.PaymentID, &attempt.BankName, &attempt.AttemptNum, &attempt.Status, &attempt.HTTPStatus); err != nil {
			return nil, err
		}
		attempts = append(attempts, attempt)
	}

	return attempts, rows.Err()
}

func (s *Store) GetPaymentDetails(ctx context.Context, paymentID int) (domain.PaymentDetails, error) {
	payment, err := s.GetPaymentByID(ctx, paymentID)
	if err != nil {
		return domain.PaymentDetails{}, err
	}

	attempts, err := s.ListPaymentAttempts(ctx, paymentID)
	if err != nil {
		return domain.PaymentDetails{}, err
	}

	return domain.PaymentDetails{
		Payment:  payment,
		Attempts: attempts,
	}, nil
}

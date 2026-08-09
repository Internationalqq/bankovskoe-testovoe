package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"bankovskoe/internal/api"
	"bankovskoe/internal/domain"
	"bankovskoe/internal/service"
	"bankovskoe/internal/storage"

	_ "github.com/lib/pq"
)

type noopPublisher struct{}

func (noopPublisher) PublishPaymentEvent(context.Context, domain.Payment) {}

func setupIntegrationTest(t *testing.T) *http.ServeMux {
	t.Helper()

	dsn := os.Getenv("INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("set INTEGRATION_DATABASE_URL to run integration tests")
	}

	database, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := database.PingContext(ctx); err != nil {
		database.Close()
		t.Fatalf("ping database: %v", err)
	}

	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_init.sql"))
	if err != nil {
		database.Close()
		t.Fatalf("read migration: %v", err)
	}
	if _, err := database.ExecContext(ctx, string(migration)); err != nil {
		database.Close()
		t.Fatalf("apply migration: %v", err)
	}
	if _, err := database.ExecContext(ctx, "TRUNCATE payment_attempts, payments RESTART IDENTITY"); err != nil {
		database.Close()
		t.Fatalf("truncate tables: %v", err)
	}

	t.Cleanup(func() {
		_, _ = database.Exec("TRUNCATE payment_attempts, payments RESTART IDENTITY")
		database.Close()
	})

	store := storage.NewStore(database)
	paymentService := service.NewPaymentService(store, service.DefaultAcquirers())
	mux := http.NewServeMux()
	api.NewHandler(paymentService, noopPublisher{}).Register(mux)
	return mux
}

func postPayment(t *testing.T, handler http.Handler, idempotencyKey string, amount int) (*httptest.ResponseRecorder, domain.PaymentResponse) {
	t.Helper()

	body, err := json.Marshal(domain.PaymentRequest{Amount: amount})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/payments", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Key", idempotencyKey)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var response domain.PaymentResponse
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode payment response: %v", err)
		}
	}

	return rec, response
}

func getPayment(t *testing.T, handler http.Handler, paymentID int) (*httptest.ResponseRecorder, domain.PaymentDetails) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/payments/%d", paymentID), nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var response domain.PaymentDetails
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode payment details: %v", err)
		}
	}

	return rec, response
}

func TestIntegrationCreatePaymentAndGetAttempts(t *testing.T) {
	handler := setupIntegrationTest(t)

	rec, created := postPayment(t, handler, "it-create-success", 1000)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /payments status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if created.Status != "SUCCESS" {
		t.Fatalf("created status = %s, want SUCCESS", created.Status)
	}

	getRec, details := getPayment(t, handler, created.ID)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET /payments/{id} status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	if details.ID != created.ID || details.Amount != 1000 || details.Status != "SUCCESS" {
		t.Fatalf("unexpected payment details: %+v", details.Payment)
	}
	if len(details.Attempts) != 1 {
		t.Fatalf("attempts len = %d, want 1", len(details.Attempts))
	}
	if details.Attempts[0].BankName != "alfa" || details.Attempts[0].Status != "SUCCESS" || details.Attempts[0].HTTPStatus != http.StatusOK {
		t.Fatalf("unexpected attempt: %+v", details.Attempts[0])
	}
}

func TestIntegrationIdempotentReplayDoesNotCreateSecondAttempt(t *testing.T) {
	handler := setupIntegrationTest(t)

	_, first := postPayment(t, handler, "it-idempotent-replay", 1000)
	replayRec, replay := postPayment(t, handler, "it-idempotent-replay", 1000)

	if replayRec.Code != http.StatusOK {
		t.Fatalf("replay status = %d, body = %s", replayRec.Code, replayRec.Body.String())
	}
	if replay.ID != first.ID {
		t.Fatalf("replay id = %d, want %d", replay.ID, first.ID)
	}
	if replayRec.Header().Get("X-Idempotency-Processed") != "true" {
		t.Fatalf("missing X-Idempotency-Processed header")
	}

	_, details := getPayment(t, handler, first.ID)
	if len(details.Attempts) != 1 {
		t.Fatalf("attempts len after replay = %d, want 1", len(details.Attempts))
	}
}

func TestIntegrationIdempotencyConflict(t *testing.T) {
	handler := setupIntegrationTest(t)

	postPayment(t, handler, "it-idempotency-conflict", 1000)
	rec, _ := postPayment(t, handler, "it-idempotency-conflict", 2000)

	if rec.Code != http.StatusConflict {
		t.Fatalf("conflicting replay status = %d, want %d, body = %s", rec.Code, http.StatusConflict, rec.Body.String())
	}
}

func TestIntegrationNoSuitableAcquirerFailsWithoutAttempts(t *testing.T) {
	handler := setupIntegrationTest(t)

	rec, created := postPayment(t, handler, "it-no-suitable-acquirer", 20000)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /payments status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if created.Status != "FAILED" {
		t.Fatalf("created status = %s, want FAILED", created.Status)
	}

	_, details := getPayment(t, handler, created.ID)
	if len(details.Attempts) != 0 {
		t.Fatalf("attempts len = %d, want 0", len(details.Attempts))
	}
}

func TestIntegrationConcurrentIdempotentRequests(t *testing.T) {
	handler := setupIntegrationTest(t)

	const workers = 8
	var wg sync.WaitGroup
	ids := make(chan int, workers)
	statuses := make(chan int, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec, response := postPayment(t, handler, "it-concurrent-idempotency", 1000)
			statuses <- rec.Code
			ids <- response.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(statuses)

	var firstID int
	for status := range statuses {
		if status != http.StatusOK {
			t.Fatalf("concurrent request status = %d, want %d", status, http.StatusOK)
		}
	}
	for id := range ids {
		if firstID == 0 {
			firstID = id
			continue
		}
		if id != firstID {
			t.Fatalf("concurrent request id = %d, want %d", id, firstID)
		}
	}

	_, details := getPayment(t, handler, firstID)
	if len(details.Attempts) != 1 {
		t.Fatalf("attempts len after concurrent requests = %d, want 1", len(details.Attempts))
	}
}

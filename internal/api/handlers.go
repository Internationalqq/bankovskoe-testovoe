package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"bankovskoe/internal/domain"
	"bankovskoe/internal/service"
)

type Publisher interface {
	PublishPaymentEvent(ctx context.Context, payment domain.Payment)
}

type Handler struct {
	service   *service.PaymentService
	publisher Publisher
}

func NewHandler(service *service.PaymentService, publisher Publisher) *Handler {
	return &Handler{
		service:   service,
		publisher: publisher,
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/payments", h.createPayment)
	mux.HandleFunc("/payments/", h.getPaymentByPath)
}

func (h *Handler) createPayment(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Idempotency-Key, X-Idempotency-Processed")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	idempotencyKey := r.Header.Get("X-Idempotency-Key")
	if strings.TrimSpace(idempotencyKey) == "" {
		http.Error(w, "Пропущен X-Idempotency-Key", http.StatusBadRequest)
		return
	}

	var req domain.PaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Плохой JSON", http.StatusBadRequest)
		return
	}
	if req.Amount <= 0 {
		http.Error(w, "Сумма платежа должна быть больше нуля", http.StatusBadRequest)
		return
	}

	fmt.Printf("\n--- Новый запрос платежа ---\n")
	fmt.Printf("Ключ идемпотентности: %s, Сумма: %d руб.\n", idempotencyKey, req.Amount)

	currentPayment, created, err := h.service.CreateOrGetProcessedPayment(r.Context(), idempotencyKey, req.Amount)
	if err != nil {
		log.Printf("[postgres] process payment transaction error: %v", err)
		http.Error(w, "Ошибка сохранения платежа", http.StatusInternalServerError)
		return
	}
	if !created {
		if currentPayment.Amount != req.Amount {
			http.Error(w, "Конфликт параметров для X-Idempotency-Key", http.StatusConflict)
			return
		}

		fmt.Printf(" [Идемпотентность]: Нашли повторный запрос! Ключ %s уже есть в базе со статусом %s. Повторно банки не вызываем.\n", idempotencyKey, currentPayment.Status)
		w.Header().Set("X-Idempotency-Processed", "true")
		w.Header().Set("Access-Control-Expose-Headers", "X-Idempotency-Processed")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(domain.PaymentResponse{ID: currentPayment.ID, Status: currentPayment.Status})
		return
	}

	h.publisher.PublishPaymentEvent(r.Context(), currentPayment)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(domain.PaymentResponse{ID: currentPayment.ID, Status: currentPayment.Status})
}

func (h *Handler) getPaymentByPath(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	idStr := strings.TrimPrefix(r.URL.Path, "/payments/")
	paymentID, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Некорректный id", http.StatusBadRequest)
		return
	}

	payment, err := h.service.GetPaymentDetails(r.Context(), paymentID)
	if err == sql.ErrNoRows {
		http.Error(w, "Платеж не найден", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("[postgres] get payment error: %v", err)
		http.Error(w, "Ошибка получения платежа", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(payment)
}

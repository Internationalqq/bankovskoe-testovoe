package service

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"sort"

	"bankovskoe/internal/domain"
	"bankovskoe/internal/storage"
)

type PaymentService struct {
	store     *storage.Store
	acquirers []domain.Acquirer
}

func DefaultAcquirers() []domain.Acquirer {
	return []domain.Acquirer{
		{
			BankName:   "alfa",
			Title:      "АО «Альфа-Банк»",
			Currencies: []string{"RUB", "USD"},
			MinSum:     1,
			MaxSum:     10000,
			Commission: 1,
			Available:  true,
			MockStatus: http.StatusOK,
		},
		{
			BankName:   "sber",
			Title:      "ПАО «Сбербанк»",
			Currencies: []string{"RUB"},
			MinSum:     1,
			MaxSum:     7000,
			Commission: 2,
			Available:  true,
			MockStatus: http.StatusInternalServerError,
		},
		{
			BankName:   "tbank",
			Title:      "АО «Т-Банк»",
			Currencies: []string{"RUB", "EUR"},
			MinSum:     100,
			MaxSum:     15000,
			Commission: 3,
			Available:  true,
			MockStatus: http.StatusBadRequest,
		},
	}
}

func NewPaymentService(store *storage.Store, acquirers []domain.Acquirer) *PaymentService {
	return &PaymentService{
		store:     store,
		acquirers: acquirers,
	}
}

func (s *PaymentService) CreateOrGetProcessedPayment(ctx context.Context, idempotencyKey string, amount int) (domain.Payment, bool, error) {
	tx, err := s.store.BeginTx(ctx)
	if err != nil {
		return domain.Payment{}, false, err
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	payment, err := s.store.CreatePaymentTx(ctx, tx, idempotencyKey, amount)
	if err == sql.ErrNoRows {
		_ = tx.Rollback()
		committed = true

		existingPayment, err := s.store.GetPaymentByIdempotencyKey(ctx, idempotencyKey)
		if err != nil {
			return domain.Payment{}, false, err
		}
		return existingPayment, false, nil
	}
	if err != nil {
		return domain.Payment{}, false, err
	}

	finalStatus, attempts := s.routePayment(payment.ID, amount)
	for _, attempt := range attempts {
		if err := s.store.InsertPaymentAttemptTx(ctx, tx, attempt); err != nil {
			return domain.Payment{}, false, err
		}
	}

	payment, err = s.store.UpdatePaymentStatusTx(ctx, tx, payment.ID, finalStatus)
	if err != nil {
		return domain.Payment{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return domain.Payment{}, false, err
	}
	committed = true

	return payment, true, nil
}

func (s *PaymentService) GetPaymentDetails(ctx context.Context, paymentID int) (domain.PaymentDetails, error) {
	return s.store.GetPaymentDetails(ctx, paymentID)
}

func (s *PaymentService) routePayment(paymentID int, amount int) (string, []domain.PaymentAttempt) {
	banks := s.sortedAcquirers(amount)
	if len(banks) == 0 {
		return domain.PaymentStatusFailed, nil
	}

	finalStatus := domain.PaymentStatusFailed
	attempts := make([]domain.PaymentAttempt, 0, len(banks))

	for idx, bank := range banks {
		attemptNum := idx + 1
		success, httpStatus := callBankWithStatus(bank.MockStatus)

		attemptRecord := domain.PaymentAttempt{
			PaymentID:  paymentID,
			BankName:   bank.BankName,
			AttemptNum: attemptNum,
			HTTPStatus: httpStatus,
		}

		if success {
			finalStatus = domain.PaymentStatusSuccess
			attemptRecord.Status = domain.AttemptStatusSuccess
			attempts = append(attempts, attemptRecord)
			break
		}

		if httpStatus == http.StatusBadRequest {
			fmt.Printf(" [Бизнес-отказ]: %s отклонил операцию (код 400). Каскад остановлен.\n", bank.BankName)
			finalStatus = domain.PaymentStatusDeclined
			attemptRecord.Status = domain.AttemptStatusDeclined
			attempts = append(attempts, attemptRecord)
			break
		}

		fmt.Printf(" [Тех. ошибка]: %s вернул код %d. Пробуем резервный банк...\n", bank.BankName, httpStatus)
		attemptRecord.Status = domain.AttemptStatusTechError
		attempts = append(attempts, attemptRecord)
	}

	return finalStatus, attempts
}

func (s *PaymentService) sortedAcquirers(amount int) []domain.Acquirer {
	var suitable []domain.Acquirer
	for _, bank := range s.acquirers {
		if amount >= bank.MinSum && amount <= bank.MaxSum {
			suitable = append(suitable, bank)
		}
	}

	sort.Slice(suitable, func(i, j int) bool {
		return suitable[i].Commission < suitable[j].Commission
	})

	return suitable
}

func callBankWithStatus(status int) (bool, int) {
	return status == http.StatusOK, status
}

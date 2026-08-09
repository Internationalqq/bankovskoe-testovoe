package domain

import "time"

const (
	PaymentStatusPending  = "PENDING"
	PaymentStatusSuccess  = "SUCCESS"
	PaymentStatusFailed   = "FAILED"
	PaymentStatusDeclined = "DECLINED"

	AttemptStatusSuccess   = "SUCCESS"
	AttemptStatusTechError = "TECH_ERROR"
	AttemptStatusDeclined  = "DECLINED"
)

type Acquirer struct {
	BankName   string   `json:"name"`
	Title      string   `json:"title"`
	Currencies []string `json:"currencies"`
	MinSum     int      `json:"min"`
	MaxSum     int      `json:"max"`
	Commission int      `json:"commission"`
	Available  bool     `json:"available"`
	MockStatus int      `json:"-"`
}

type Payment struct {
	ID             int    `json:"id"`
	IdempotencyKey string `json:"idempotency_key"`
	Amount         int    `json:"amount"`
	Status         string `json:"status"`
}

type PaymentAttempt struct {
	ID         int    `json:"id"`
	PaymentID  int    `json:"payment_id"`
	BankName   string `json:"bank_name"`
	AttemptNum int    `json:"attempt_num"`
	Status     string `json:"status"`
	HTTPStatus int    `json:"http_status"`
}

type PaymentDetails struct {
	Payment
	Attempts []PaymentAttempt `json:"attempts"`
}

type PaymentRequest struct {
	Amount int `json:"amount"`
}

type PaymentResponse struct {
	ID     int    `json:"id"`
	Status string `json:"status"`
}

type PaymentEvent struct {
	ID             int       `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	Amount         int       `json:"amount"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
}

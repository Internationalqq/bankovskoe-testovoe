package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const kafkaTopic = "payment_events"

// Структура банка (Эквайера)
type Acquirer struct {
	BankName   string   `json:"name"`
	Title      string   `json:"title"`
	Currencies []string `json:"currencies"`
	MinSum     int      `json:"min"`
	MaxSum     int      `json:"max"`
	Commission int      `json:"commission"`
	Available  bool     `json:"available"`
	MockURL    string   `json:"-"` // Скрываем урл от фронтенда ради безопасности
}

// Структура для хранения платежа в нашей "базе данных"
type Payment struct {
	ID             int
	IdempotencyKey string
	Amount         int
	Status         string // PENDING, SUCCESS, FAILED
}

// Структура для истории попыток
type PaymentAttempt struct {
	PaymentID  int
	BankName   string
	AttemptNum int
	Status     string // SUCCESS, TECH_ERROR
	HTTPStatus int
}

// Наша фейковая база данных в памяти (просто слайсы)
var paymentsDB = []Payment{}
var attemptsDB = []PaymentAttempt{}
var nextPaymentID = 1 // Счетчик для ID платежей

// Создаем список наших доступных банков (базу данных банков)
var availableAcquirers = []Acquirer{
	{
		BankName:   "alfa", // Настоящее системное имя (ID)
		Title:      "АО «Альфа-Банк»",
		Currencies: []string{"RUB", "USD"},
		MinSum:     1,
		MaxSum:     10000,
		Commission: 1,
		Available:  true,
		MockURL:    "http://localhost:8090/mock-bank?status=200",
	},
	{
		BankName:   "sber", // Настоящее системное имя (ID)
		Title:      "ПАО «Сбербанк»",
		Currencies: []string{"RUB"},
		MinSum:     1,
		MaxSum:     7000,
		Commission: 2,
		Available:  true,
		MockURL:    "http://localhost:8090/mock-bank?status=500",
	},
	{
		BankName:   "tbank", // Настоящее системное имя (ID)
		Title:      "АО «Т-Банк»",
		Currencies: []string{"RUB", "EUR"},
		MinSum:     100,
		MaxSum:     15000,
		Commission: 3,
		Available:  true,
		MockURL:    "http://localhost:8090/mock-bank?status=400",
	},
}

// Описываем, какой JSON к нам прилетит с фронтенда
type PaymentRequest struct {
	Amount int `json:"amount"` // Сумма платежа (например, 1000)
}

// Описываем, какой JSON мы вернем фронтенду в ответ
type PaymentResponse struct {
	ID     int    `json:"id"`
	Status string `json:"status"` // READY, PROCESSING, SUCCESS, FAILED
}

type PaymentEvent struct {
	ID             int       `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	Amount         int       `json:"amount"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
}

func kafkaBrokers() []string {
	raw := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if raw == "" {
		return []string{"localhost:9092"}
	}

	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}
	if len(brokers) == 0 {
		return []string{"localhost:9092"}
	}

	return brokers
}

func publishPaymentEvent(ctx context.Context, payment Payment) {
	value, err := json.Marshal(PaymentEvent{
		ID:             payment.ID,
		IdempotencyKey: payment.IdempotencyKey,
		Amount:         payment.Amount,
		Status:         payment.Status,
		CreatedAt:      time.Now(),
	})
	if err != nil {
		log.Printf("[kafka] marshal payment event error: %v", err)
		return
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBrokers()...),
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	kafkaCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	err = writer.WriteMessages(kafkaCtx, kafka.Message{
		Key:   []byte(strconv.Itoa(payment.ID)),
		Value: value,
	})
	if err != nil {
		log.Printf("[kafka] publish payment event error: %v", err)
		return
	}

	log.Printf("[kafka] published payment event: %s", value)
}

func startKafkaListener(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  kafkaBrokers(),
		Topic:    kafkaTopic,
		GroupID:  "bankovskoe-payment-listener",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	go func() {
		defer reader.Close()
		log.Printf("[kafka] listener started: topic=%s brokers=%s", kafkaTopic, strings.Join(kafkaBrokers(), ","))

		for {
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[kafka] listener read error: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			log.Printf("[kafka] received topic=%s partition=%d offset=%d key=%s value=%s",
				message.Topic,
				message.Partition,
				message.Offset,
				string(message.Key),
				string(message.Value),
			)
		}
	}()
}

// Функция, которая выбирает банки по сумме и минимальной комиссии
func getSortedAcquirers(amount int) []Acquirer {
	var suitable []Acquirer

	// 1. Отбираем все банки, которые подходят по лимитам суммы
	for _, bank := range availableAcquirers {
		if amount >= bank.MinSum && amount <= bank.MaxSum {
			suitable = append(suitable, bank)
		}
	}

	// 2. Сортируем их по комиссии: самый дешевый банк будет первым в списке, дорогой — в конце
	sort.Slice(suitable, func(i, j int) bool {
		return suitable[i].Commission < suitable[j].Commission
	})

	return suitable
}

// Функция-обработчик (Handler) для адреса /pay
func payHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Idempotency-Key, X-Idempotency-Processed")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET") // Добавили GET для будущего запроса статуса

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	idempotencyKey := r.Header.Get("X-Idempotency-Key")

	var req PaymentRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Плохой JSON", http.StatusBadRequest)
		return
	}

	fmt.Printf("\n--- Новый запрос платежа ---\n")
	fmt.Printf("Ключ идемпотентности: %s, Сумма: %d руб.\n", idempotencyKey, req.Amount)

	// ИДЕМПОТЕНТНОСТЬ: Ищем в базе, не обрабатывали ли мы уже платеж с таким ключом?
	for _, existingPayment := range paymentsDB {
		if existingPayment.IdempotencyKey == idempotencyKey {
			fmt.Printf(" [Идемпотентность]: Нашли повторный запрос! Ключ %s уже есть в базе со статусом %s. Повторно банки не вызываем.\n", idempotencyKey, existingPayment.Status)

			// Добавляем специальный флаг в заголовки ответа для фронтенда!
			w.Header().Set("X-Idempotency-Processed", "true")
			w.Header().Set("Access-Control-Expose-Headers", "X-Idempotency-Processed") // Разрешаем браузеру читать этот заголовок

			// Сразу отдаем старый ответ фронтенду, не заходя в цикл банков!
			resp := PaymentResponse{
				ID:     existingPayment.ID,
				Status: existingPayment.Status,
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return // ВЫХОДИМ ИЗ ФУНКЦИИ, гасим повторный запрос
		}
	}

	// 1. Создаем запись о новом платеже в нашей "базе данных"
	currentPayment := Payment{
		ID:             nextPaymentID,
		IdempotencyKey: idempotencyKey,
		Amount:         req.Amount,
		Status:         "PENDING", // Изначально платеж в обработке
	}
	paymentsDB = append(paymentsDB, currentPayment)
	nextPaymentID++ // Увеличиваем счетчик для следующего платежа

	banks := getSortedAcquirers(req.Amount)
	if len(banks) == 0 {
		// Если банков нет, сразу переводим платеж в FAILED
		currentPayment.Status = "FAILED"
		paymentsDB[len(paymentsDB)-1].Status = "FAILED"
		publishPaymentEvent(r.Context(), currentPayment)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(PaymentResponse{ID: currentPayment.ID, Status: "FAILED"})
		return
	}

	finalStatus := "FAILED"

	// 2. Цикл каскадирования
	for idx, bank := range banks {
		attemptNum := idx + 1
		success, httpStatus := callBankHTTPWithStatus(bank.MockURL)

		attemptRecord := PaymentAttempt{
			PaymentID:  currentPayment.ID,
			BankName:   bank.BankName,
			AttemptNum: attemptNum,
			HTTPStatus: httpStatus,
		}

		if success {
			finalStatus = "SUCCESS"
			attemptRecord.Status = "SUCCESS"
			attemptsDB = append(attemptsDB, attemptRecord)
			break
		}

		// Если банк вернул 400 — это бизнес-отказ (нет денег / заблокирована карта)
		if httpStatus == 400 {
			fmt.Printf(" [Бизнес-отказ]: %s отклонил операцию (код 400). Каскад остановлен.\n", bank.BankName)
			finalStatus = "DECLINED"
			attemptRecord.Status = "DECLINED"
			attemptsDB = append(attemptsDB, attemptRecord)
			break // СТОПАЕМ ЦИКЛ, дальше идти нет смысла
		}

		// Если код 500 или 0 (сеть) — это тех. ошибка, идем на следующий круг к резервному банку
		fmt.Printf(" [Тех. ошибка]: %s вернул код %d. Пробуем резервный банк...\n", bank.BankName, httpStatus)
		attemptRecord.Status = "TECH_ERROR"
		attemptsDB = append(attemptsDB, attemptRecord)
	}

	// 3. Обновляем финальный статус платежа в нашей базе данных
	// Находим наш платеж по индексу в слайсе и меняем статус
	for i, p := range paymentsDB {
		if p.ID == currentPayment.ID {
			paymentsDB[i].Status = finalStatus
			break
		}
	}
	currentPayment.Status = finalStatus
	publishPaymentEvent(r.Context(), currentPayment)

	// Выводим текущее состояние "базы" в консоль для проверки
	fmt.Printf("=> ПЛАТЕЖИ В БАЗЕ: %v\n", paymentsDB)
	fmt.Printf("=> ПОПЫТКИ В БАЗЕ: %v\n", attemptsDB)

	// Отдаем ответ фронтенду
	resp := PaymentResponse{
		ID:     currentPayment.ID,
		Status: finalStatus,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Функция делает реальный запрос в банк и возвращает true (если успех) или false (если ошибка банка)
func callBankHTTPWithStatus(url string) (bool, int) {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Post(url, "application/json", nil)
	if err != nil {
		return false, 0 // Ошибка сети
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		return true, resp.StatusCode
	}
	return false, resp.StatusCode
}

// FAKE mock-bank
func mockBankHandler(w http.ResponseWriter, r *http.Request) {
	// Читаем из параметров запроса, какой статус мы хотим сымитировать
	status := r.URL.Query().Get("status")

	if status == "500" {
		w.WriteHeader(http.StatusInternalServerError) // Имитируем сбой банка
		return
	}
	if status == "200" {
		w.WriteHeader(http.StatusOK) // Имитируем успех
		return
	}

	w.WriteHeader(http.StatusBadRequest)
}

// Получить статус на Фронт
func getStatusHandler(w http.ResponseWriter, r *http.Request) {
	// Разрешаем CORS
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

	// Достаем ID платежа из URL. Например: /payment/status?id=1
	idStr := r.URL.Query().Get("id")
	if idStr == "" {
		http.Error(w, "Пропущен параметр id", http.StatusBadRequest)
		return
	}

	// Переводим id из строки в число
	paymentID, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Некорректный id", http.StatusBadRequest)
		return
	}

	// Ищем платеж в нашей "базе данных"
	var foundPayment Payment
	found := false
	for _, p := range paymentsDB {
		if p.ID == paymentID {
			foundPayment = p
			found = true
			break
		}
	}

	// Если не нашли платеж с таким ID
	if !found {
		http.Error(w, "Платеж не найден", http.StatusNotFound)
		return
	}

	// Если нашли — отдаем его обратно в формате JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(foundPayment)
}

func getPaymentsHandler(w http.ResponseWriter, r *http.Request) {
	// Настройки CORS
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

	// Просто отдаем весь слайс платежей обратно фронтенду
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(paymentsDB)
}

func getBanksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(availableAcquirers)
}

func main() {
	startKafkaListener(context.Background())

	http.HandleFunc("/pay", payHandler)                  // Регистрируем наш обработчик на URL /pays
	http.HandleFunc("/payment/status", getStatusHandler) // Добавили эту строчку!
	http.HandleFunc("/payments", getPaymentsHandler)
	http.HandleFunc("/banks", getBanksHandler)
	http.HandleFunc("/mock-bank", mockBankHandler) // Регистрируем фейковый банк прямо у себя на сервере

	fmt.Println("Go-бэкенд успешно запущен на порту :8090...")

	// Запускаем сервер на порту 8090, как просит фронтенд
	log.Fatal(http.ListenAndServe(":8090", nil))
}

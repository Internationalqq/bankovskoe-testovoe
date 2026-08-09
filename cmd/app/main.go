package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"

	"bankovskoe/internal/api"
	eventskafka "bankovskoe/internal/kafka"
	"bankovskoe/internal/service"
	"bankovskoe/internal/storage"
)

func databaseURL() string {
	raw := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if raw != "" {
		return raw
	}

	return "postgres://bankovskoe:bankovskoe@localhost:5432/bankovskoe?sslmode=disable"
}

func main() {
	// Ага
	ctx := context.Background()

	db, err := storage.Connect(ctx, databaseURL())
	if err != nil {
		log.Fatalf("[postgres] connect error: %v", err)
	}
	defer db.Close()

	brokers := eventskafka.BrokersFromEnv()
	if !eventskafka.DisabledFromEnv() {
		eventskafka.StartListener(ctx, brokers, eventskafka.DefaultTopic)
	}

	store := storage.NewStore(db)
	paymentService := service.NewPaymentService(store, service.DefaultAcquirers())
	producer := &eventskafka.Producer{
		Brokers:  brokers,
		Topic:    eventskafka.DefaultTopic,
		Disabled: eventskafka.DisabledFromEnv(),
	}

	mux := http.NewServeMux()
	api.NewHandler(paymentService, producer).Register(mux)

	log.Println("Go-бэкенд успешно запущен на порту :8090...")
	log.Fatal(http.ListenAndServe(":8090", mux))
}

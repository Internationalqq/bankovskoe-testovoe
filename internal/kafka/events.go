package kafka

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"bankovskoe/internal/domain"

	kafkago "github.com/segmentio/kafka-go"
)

const DefaultTopic = "payment_events"

type Producer struct {
	Brokers  []string
	Topic    string
	Disabled bool
}

func BrokersFromEnv() []string {
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

func DisabledFromEnv() bool {
	return strings.EqualFold(os.Getenv("KAFKA_DISABLED"), "true") || os.Getenv("KAFKA_DISABLED") == "1"
}

func (p *Producer) PublishPaymentEvent(ctx context.Context, payment domain.Payment) {
	if p.Disabled {
		return
	}

	topic := p.Topic
	if topic == "" {
		topic = DefaultTopic
	}

	value, err := json.Marshal(domain.PaymentEvent{
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

	writer := &kafkago.Writer{
		Addr:     kafkago.TCP(p.Brokers...),
		Topic:    topic,
		Balancer: &kafkago.LeastBytes{},
	}
	defer writer.Close()

	kafkaCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	err = writer.WriteMessages(kafkaCtx, kafkago.Message{
		Key:   []byte(strconv.Itoa(payment.ID)),
		Value: value,
	})
	if err != nil {
		log.Printf("[kafka] publish payment event error: %v", err)
		return
	}

	log.Printf("[kafka] published payment event: %s", value)
}

func StartListener(ctx context.Context, brokers []string, topic string) {
	if topic == "" {
		topic = DefaultTopic
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  "bankovskoe-payment-listener",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	go func() {
		defer reader.Close()
		log.Printf("[kafka] listener started: topic=%s brokers=%s", topic, strings.Join(brokers, ","))

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

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os/signal"
	"syscall"
	"time"

	"involta/internal/processor"
	"involta/internal/storage"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

const (
	broker = "localhost:9092"
	topic  = "user-events"
	dsn    = "postgres://user:password@localhost:5432/notifications?sslmode=disable"
)

func main() {
	store, err := storage.New(dsn)
	if err != nil {
		log.Fatal("DB connection error:", err)
	}
	defer store.Close()

	if err := store.Setup(); err != nil {
		log.Fatal("Migration error:", err)
	}

	svc := processor.New(store)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: "worker-1",
		MaxWait: 1 * time.Second,
	})
	defer r.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go produceMockData(ctx)

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("Kafka connection error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if err := svc.Process(ctx, m.Value); err != nil {
			log.Printf("Processing error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if err := r.CommitMessages(ctx, m); err != nil {
			log.Printf("Commit error: %v", err)
		}
	}
}

func produceMockData(ctx context.Context) {
	time.Sleep(5 * time.Second)

	w := &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	events := []string{"order_created", "payment_received", "order_shipped"}
	log.Println("Generator started")

	for i := range 10 {
		select {
		case <-ctx.Done():
			return
		default:
			payload := `{"valid":true}`
			if i == 8 {
				payload = `{"broken":`
			}

			eventID := uuid.New().String()

			msg := fmt.Sprintf(`{"event_id":"%s","user_id":%d,"event_type":"%s","payload":%s}`,
				eventID, 100+i, events[rand.Intn(len(events))], payload)

			err := w.WriteMessages(ctx, kafka.Message{Value: []byte(msg)})
			if err != nil {
				log.Printf("Failed to send: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			log.Printf("Sent event %d (ID: %s)", i, eventID)

			time.Sleep(500 * time.Millisecond)
		}
	}
	log.Println("Generator finished")
}

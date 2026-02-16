package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"involta/internal/models"
	"involta/internal/storage"
	"log"
	"math"
	"math/rand"
	"time"
)

var (
	ErrTemp      = errors.New("temp error")
	ErrPerm      = errors.New("perm error")
	ErrStopRetry = errors.New("stop retry")
)

type Repository interface {
	SaveEvent(ctx context.Context, eventID string) error
}

type Service struct {
	repo Repository
}

func New(r Repository) *Service {
	return &Service{repo: r}
}

func (s *Service) send(channel, msg string) error {
	r := rand.Float64()
	if r < 0.01 {
		return ErrPerm
	} // 1% fatal
	if r < 0.11 {
		return ErrTemp
	} // 10% temp
	log.Printf("[SENT %s] %s", channel, msg)
	return nil
}

func (s *Service) tryDeliver(channel string, e models.Event) error {
	for i := 0; i <= 3; i++ {
		err := s.send(channel, fmt.Sprintf("%s user:%d", e.EventType, e.UserID))
		if err == nil {
			return nil
		}

		if errors.Is(err, ErrPerm) || i == 3 {
			log.Printf("Failed %s: %v", e.EventID, err)
			return ErrStopRetry
		}
		time.Sleep(200 * time.Millisecond * time.Duration(math.Pow(2, float64(i))))
	}
	return nil
}

func (s *Service) Process(ctx context.Context, val []byte) error {
	var e models.Event
	if err := json.Unmarshal(val, &e); err != nil {
		log.Printf("Invalid JSON, skipping")
		return nil
	}

	if err := s.repo.SaveEvent(ctx, e.EventID); err != nil {
		if errors.Is(err, storage.ErrDuplicate) {
			log.Printf("Duplicate event, skipping: %s", e.EventID)
			return nil
		}
		return err
	}

	var chans []string
	switch e.EventType {
	case "order_created":
		chans = []string{"email", "push"}
	case "payment_received":
		chans = []string{"email"}
	case "order_shipped":
		chans = []string{"push", "sms"}
	default:
		return nil
	}

	for _, ch := range chans {
		if err := s.tryDeliver(ch, e); err != nil && !errors.Is(err, ErrStopRetry) {
			return err
		}
	}

	return nil
}

package main

import (
	"context"
	"log"
	"time"

	"audit-service/internal/producer"

	"github.com/google/uuid"
)

func main() {
	p := producer.NewProducer([]string{"localhost:9092"}, "user-actions")

	event := producer.UserActionEvent{
		EventID:    uuid.New().String(),
		UserID:     "user1",
		Action:     "login",
		ResourceID: "resource1",
		Timestamp:  time.Now().UTC(),
	}

	if err := p.SendEvent(context.Background(), event); err != nil {
		log.Fatalf("failed to send event: %v", err)
	}

	log.Println("event sent successfully")
}

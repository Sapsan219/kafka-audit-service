package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr       string
	DatabaseURL    string
	KafkaBrokers   []string
	KafkaTopic     string
	ShutdownPeriod time.Duration
}

func Load() (Config, error) {
	shutdownPeriod, err := time.ParseDuration(env("SHUTDOWN_PERIOD", "10s"))
	if err != nil {
		return Config{}, fmt.Errorf("parse SHUTDOWN_PERIOD: %w", err)
	}

	return Config{
		HTTPAddr:       env("HTTP_ADDR", ":8080"),
		DatabaseURL:    env("DATABASE_URL", "postgres://audit:audit@127.0.0.1:5433/audit?sslmode=disable"),
		KafkaBrokers:   strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:     env("KAFKA_TOPIC", "user-actions"),
		ShutdownPeriod: shutdownPeriod,
	}, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

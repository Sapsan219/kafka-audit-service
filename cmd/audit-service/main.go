package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"audit-service/internal/config"
	"audit-service/internal/consumer"
	"audit-service/internal/handler"
	"audit-service/internal/producer"
	"audit-service/internal/replay"
	"audit-service/internal/repository"
	"audit-service/internal/service"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		log.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Error("ping PostgreSQL", "error", err)
		os.Exit(1)
	}

	eventProducer, err := producer.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		log.Error("connect to Kafka", "error", err)
		os.Exit(1)
	}
	defer eventProducer.Close()

	auditRepository := repository.NewAuditRepository(pool)
	auditService := service.NewAuditService(auditRepository, eventProducer)
	statsReplayer, err := replay.New(cfg.KafkaBrokers, cfg.KafkaTopic, auditRepository)
	if err != nil {
		log.Error("connect Kafka replay consumer", "error", err)
		os.Exit(1)
	}
	defer statsReplayer.Close()
	httpHandler := handler.New(auditService, statsReplayer, log)

	analyticsConsumer, err := consumer.New(
		cfg.KafkaBrokers,
		cfg.KafkaGroupID,
		cfg.KafkaTopic,
		cfg.KafkaBatchSize,
		cfg.CommitInterval,
		cfg.AnalyticsInterval,
		auditRepository,
		log,
	)
	if err != nil {
		log.Error("connect Kafka consumer", "error", err)
		os.Exit(1)
	}
	defer analyticsConsumer.Close()

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpHandler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("HTTP server started", "address", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server failed", "error", err)
			stop()
		}
	}()

	go func() {
		if err := analyticsConsumer.Run(ctx); err != nil {
			log.Error("analytics consumer failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownPeriod)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("HTTP server shutdown", "error", err)
	}
	log.Info("service stopped")
}

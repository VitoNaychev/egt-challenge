package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VitoNaychev/egt-challenge/persistence/consumer"
	eventsvcpb "github.com/VitoNaychev/egt-challenge/persistence/gen"
	"github.com/VitoNaychev/egt-challenge/persistence/rpc"
	"github.com/VitoNaychev/egt-challenge/persistence/repo"
	"github.com/VitoNaychev/egt-challenge/persistence/service"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type Config struct {
	LogLevel                 string        `mapstructure:"LOG_LEVEL"`
	GRPCAddr                 string        `mapstructure:"GRPC_ADDR"`
	KafkaBrokers             []string      `mapstructure:"KAFKA_BROKERS"`
	KafkaTopic               string        `mapstructure:"KAFKA_TOPIC"`
	KafkaGroupID             string        `mapstructure:"KAFKA_GROUP_ID"`
	DatabaseURL              string        `mapstructure:"DATABASE_URL"`
	ConsumerUnknownErrorRetryBudget int           `mapstructure:"CONSUMER_UNKNOWN_ERROR_RETRY_BUDGET"`
	ConsumerBackoffDuration      time.Duration `mapstructure:"CONSUMER_BACKOFF_DURATION"`
	ConsumerMaxBackoff           time.Duration `mapstructure:"CONSUMER_MAX_BACKOFF"`
}

func loadConfig() (Config, error) {
	v := viper.NewWithOptions(viper.ExperimentalBindStruct())
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal config: %w", err)
	}
	if cfg.LogLevel == "" {
		return Config{}, errors.New("LOG_LEVEL must be set")
	}
	if cfg.GRPCAddr == "" {
		return Config{}, errors.New("GRPC_ADDR must be set")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, errors.New("KAFKA_BROKERS must be set")
	}
	if cfg.KafkaTopic == "" {
		return Config{}, errors.New("KAFKA_TOPIC must be set")
	}
	if cfg.KafkaGroupID == "" {
		return Config{}, errors.New("KAFKA_GROUP_ID must be set")
	}
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL must be set")
	}
	if cfg.ConsumerUnknownErrorRetryBudget <= 0 {
		return Config{}, errors.New("CONSUMER_UNKNOWN_ERROR_RETRY_BUDGET must be set to a positive integer")
	}
	if cfg.ConsumerBackoffDuration <= 0 {
		return Config{}, errors.New("CONSUMER_BACKOFF_DURATION must be set to a positive duration")
	}
	if cfg.ConsumerMaxBackoff < cfg.ConsumerBackoffDuration {
		return Config{}, errors.New("CONSUMER_MAX_BACKOFF must be set to a duration >= CONSUMER_BACKOFF_DURATION")
	}
	return cfg, nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if err := run(); err != nil {
		slog.Error("service exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return fmt.Errorf("parse LOG_LEVEL: %w", err)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("create pgx pool: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaGroupID,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			slog.Error("close kafka reader", "error", err)
		}
	}()

	eventRepo := repo.NewEventRepository(pool)
	svc := service.NewEventService(eventRepo)
	consumerConfig := consumer.Config{
		UnknownErrorRetryBudget: cfg.ConsumerUnknownErrorRetryBudget,
		BackoffDuration:      cfg.ConsumerBackoffDuration,
		MaxBackoff:           cfg.ConsumerMaxBackoff,
	}
	cons := consumer.NewKafkaConsumer(consumerConfig, reader, svc, slog.Default().With("component", "consumer"))

	grpcServer := grpc.NewServer()
	eventsvcpb.RegisterEventServiceServer(grpcServer, rpc.NewEventHandler(svc, slog.Default().With("component", "grpc")))

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	reflection.Register(grpcServer)

	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}

	errCh := make(chan error, 2)
	go func() {
		slog.Info("grpc server listening", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(grpcListener); err != nil {
			errCh <- fmt.Errorf("serve grpc: %w", err)
		}
	}()
	go func() {
		slog.Info("consumer started", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID)
		if err := cons.Run(ctx); err != nil {
			errCh <- fmt.Errorf("run consumer: %w", err)
			return
		}
		errCh <- nil
	}()

	err = <-errCh
	slog.Info("shutting down")

	stop()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	grpcServer.GracefulStop()
	return err
}

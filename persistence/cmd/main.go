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

	"github.com/VitoNaychev/egt-challenge/persistence/consumer"
	eventpb "github.com/VitoNaychev/egt-challenge/persistence/gen"
	grpchandler "github.com/VitoNaychev/egt-challenge/persistence/grpc"
	"github.com/VitoNaychev/egt-challenge/persistence/repo"
	"github.com/VitoNaychev/egt-challenge/persistence/service"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type Config struct {
	LogLevel     string   `mapstructure:"LOG_LEVEL"`
	GRPCAddr     string   `mapstructure:"GRPC_ADDR"`
	KafkaBrokers []string `mapstructure:"KAFKA_BROKERS"`
	KafkaTopic   string   `mapstructure:"KAFKA_TOPIC"`
	KafkaGroupID string   `mapstructure:"KAFKA_GROUP_ID"`
	DatabaseURL  string   `mapstructure:"DATABASE_URL"`
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
	cons := consumer.NewKafkaConsumer(reader, svc)

	grpcServer := grpc.NewServer()
	eventpb.RegisterEventServiceServer(grpcServer, grpchandler.NewEventHandler(svc))

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

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

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VitoNaychev/egt-challenge/ingestion/handler"
	"github.com/VitoNaychev/egt-challenge/ingestion/publisher"
	"github.com/VitoNaychev/egt-challenge/ingestion/service"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type Config struct {
	ListenAddr      string        `mapstructure:"LISTEN_ADDR"`
	GRPCAddr        string        `mapstructure:"GRPC_ADDR"`
	KafkaBrokers    []string      `mapstructure:"KAFKA_BROKERS"`
	KafkaTopic      string        `mapstructure:"KAFKA_TOPIC"`
	PublishTimeout  time.Duration `mapstructure:"PUBLISH_TIMEOUT"`
	ShutdownTimeout time.Duration `mapstructure:"SHUTDOWN_TIMEOUT"`
}

func loadConfig() (Config, error) {
	v := viper.NewWithOptions(viper.ExperimentalBindStruct())
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal config: %w", err)
	}
	if cfg.ListenAddr == "" {
		return Config{}, errors.New("LISTEN_ADDR must be set")
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
	if cfg.PublishTimeout <= 0 {
		return Config{}, errors.New("PUBLISH_TIMEOUT must be set to a positive duration")
	}
	if cfg.ShutdownTimeout <= 0 {
		return Config{}, errors.New("SHUTDOWN_TIMEOUT must be set to a positive duration")
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

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		// debug convenience - lets the broker create the topic on first publish
		AllowAutoTopicCreation: true,
	}
	defer func() {
		if err := writer.Close(); err != nil {
			slog.Error("close kafka writer", "error", err)
		}
	}()

	pub := publisher.NewKafkaPublisher(writer, publisher.WithPublishTimeout(cfg.PublishTimeout))
	svc := service.NewEventService(pub)

	mux := http.NewServeMux()
	mux.Handle("/events", handler.NewEventHandler(svc))

	srv := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: mux,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("ingestion service listening", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("listen and serve: %w", err)
		}
	}()

	grpcServer := grpc.NewServer()

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}
	go func() {
		slog.Info("grpc health server listening", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(grpcListener); err != nil {
			errCh <- fmt.Errorf("serve grpc: %w", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case sig := <-stop:
		slog.Info("shutting down", "signal", sig.String())
	}

	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown server: %w", err)
	}
	grpcServer.GracefulStop()
	return nil
}

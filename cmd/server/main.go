package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.opentelemetry.io/contrib/instrumentation/github.com/labstack/echo/otelecho"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// ====== Config via env ======
	serviceName := getenv("OTEL_SERVICE_NAME", "echo-otel-demo")
	otlpEndpoint := getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otelcol-collector.monitoring.svc.cluster.local:4317")
	// Optional: set to "dev", "staging", "prod"
	environment := getenv("SERVICE_ENVIRONMENT", "dev")

	ctx := context.Background()

	tp, err := initTracerProvider(ctx, serviceName, otlpEndpoint, environment)
	if err != nil {
		log.Fatalf("init tracer provider: %v", err)
	}
	defer func() {
		_ = tp.Shutdown(context.Background())
	}()

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())
	e.Use(middleware.RequestID())

	// Auto create server spans per HTTP request
	e.Use(otelecho.Middleware(serviceName))

	e.GET("/healthz", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{"ok": true})
	})

	e.GET("/ping", func(c echo.Context) error {
		// This endpoint will produce a trace span automatically via middleware
		return c.JSON(http.StatusOK, map[string]any{
			"message": "pong",
			"time":    time.Now().Format(time.RFC3339Nano),
		})
	})

	// Endpoint to simulate "end-to-end" work (child spans + random error)
	e.GET("/work", func(c echo.Context) error {
		ctx := c.Request().Context()
		tr := otel.Tracer("echo-otel-demo")

		// Child span #1: pretend doing business logic
		_, span := tr.Start(ctx, "business_logic",
			trace.WithAttributes(attribute.String("feature", "demo_work")),
		)
		sleepMs := rand.Intn(250) + 50
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		span.SetAttributes(attribute.Int("sleep_ms", sleepMs))
		span.End()

		// Child span #2: pretend doing outbound HTTP call
		_, span2 := tr.Start(ctx, "outbound_call")
		// Just call local ping for demo (still shows as child span)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:8080/ping", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			span2.RecordError(err)
			span2.SetStatus(1, "outbound failed")
			span2.End()
			return c.JSON(http.StatusBadGateway, map[string]any{"error": err.Error()})
		}
		_ = resp.Body.Close()
		span2.SetAttributes(attribute.Int("outbound_status", resp.StatusCode))
		span2.End()

		// Random error to test error traces
		if rand.Intn(10) == 0 {
			err := errors.New("simulated error (1/10)")
			spanErr := trace.SpanFromContext(ctx)
			spanErr.RecordError(err)
			return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
		}

		return c.JSON(http.StatusOK, map[string]any{
			"result":  "ok",
			"sleepMs": sleepMs,
		})
	})

	// Graceful shutdown
	go func() {
		addr := ":8080"
		log.Printf("listening on %s", addr)
		if err := e.Start(addr); err != nil && err != http.ErrServerClosed {
			log.Fatalf("echo start: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	ctxShutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = e.Shutdown(ctxShutdown)
}

func initTracerProvider(ctx context.Context, serviceName, otlpEndpoint, environment string) (*sdktrace.TracerProvider, error) {
	// OTLP gRPC exporter (to otelcol)
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", serviceName),
			attribute.String("deployment.environment", environment),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exp),
		// sample all for demo; for prod you can lower this
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	return tp, nil
}

func getenv(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.opentelemetry.io/contrib/instrumentation/github.com/labstack/echo/otelecho"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
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
	downstreamURL := getenv("DOWNSTREAM_URL", "http://127.0.0.1:8080/ping")

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
		fmt.Println("hit /healthz")

		return c.JSON(http.StatusOK, map[string]any{"ok": true})
	})

	e.GET("/ping", func(c echo.Context) error {
		fmt.Println("hit /ping")

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

		// Add baggage for demo (will appear in trace context)
		member, _ := baggage.NewMember("tenant", "davdev")
		bg, _ := baggage.New(member)
		ctx = baggage.ContextWithBaggage(ctx, bg)

		ctx, root := tr.Start(ctx, "work",
			trace.WithAttributes(
				attribute.String("feature", "demo_work"),
				attribute.String("env", environment),
				attribute.String("route", "/work"),
			),
		)
		defer root.End()

		root.AddEvent("work.start")

		// Simulated cache lookup
		doSleepSpan(ctx, tr, "cache.get", 5, 25,
			attribute.String("cache.system", "redis"),
			attribute.String("cache.key", "user:123"),
		)

		// Simulated DB query
		doSleepSpan(ctx, tr, "db.query", 30, 120,
			attribute.String("db.system", "postgresql"),
			attribute.String("db.name", "kong"),
			attribute.String("db.operation", "SELECT"),
		)

		// CPU work span (simulate compute)
		doSleepSpan(ctx, tr, "compute.hash", 20, 80,
			attribute.String("algo", "bcrypt"),
		)

		// Downstream call (distributed trace if downstream also instruments + propagator)
		status, err := httpCallSpan(ctx, tr, downstreamURL)
		if err != nil {
			root.RecordError(err)
			root.SetStatus(codes.Error, "downstream failed")
			return c.JSON(http.StatusBadGateway, map[string]any{
				"error": err.Error(),
			})
		}

		// Random error
		if rand.Intn(10) == 0 {
			err := errors.New("simulated error (1/10)")
			root.RecordError(err)
			root.SetStatus(codes.Error, "simulated error")
			return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
		}

		root.AddEvent("work.done", trace.WithAttributes(attribute.Int("downstream_status", status)))
		root.SetStatus(codes.Ok, "ok")

		return c.JSON(http.StatusOK, map[string]any{
			"result":           "ok",
			"downstreamStatus": status,
			"downstreamURL":    downstreamURL,
		})
	})

	e.GET("/work/parallel", func(c echo.Context) error {
		ctx := c.Request().Context()
		tr := otel.Tracer("echo-otel-demo")

		ctx, root := tr.Start(ctx, "work.parallel")
		defer root.End()

		var wg sync.WaitGroup
		wg.Add(3)

		go func() { defer wg.Done(); doSleepSpan(ctx, tr, "parallel.task.a", 50, 150) }()
		go func() { defer wg.Done(); doSleepSpan(ctx, tr, "parallel.task.b", 30, 120) }()
		go func() { defer wg.Done(); doSleepSpan(ctx, tr, "parallel.task.c", 80, 200) }()

		wg.Wait()
		root.SetStatus(codes.Ok, "ok")

		return c.JSON(http.StatusOK, map[string]any{"result": "ok"})
	})

	e.GET("/work/slow", func(c echo.Context) error {
		ctx := c.Request().Context()
		tr := otel.Tracer("echo-otel-demo")

		ctx, sp := tr.Start(ctx, "work.slow")
		defer sp.End()

		doSleepSpan(ctx, tr, "slow.sleep", 800, 1500)
		sp.SetStatus(codes.Ok, "ok")
		return c.JSON(http.StatusOK, map[string]any{"result": "slow-ok"})
	})

	e.GET("/work/error", func(c echo.Context) error {
		ctx := c.Request().Context()
		tr := otel.Tracer("echo-otel-demo")

		ctx, sp := tr.Start(ctx, "work.error")
		defer sp.End()

		err := errors.New("always error for demo")
		sp.RecordError(err)
		sp.SetStatus(codes.Error, "forced error")
		return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
	})

	e.GET("/work/chain", func(c echo.Context) error {
		ctx := c.Request().Context()
		tr := otel.Tracer("echo-otel-demo")

		ctx, sp := tr.Start(ctx, "chain.root")
		defer sp.End()

		// child 1
		ctx1, s1 := tr.Start(ctx, "chain.step1")
		doSleepSpan(ctx1, tr, "chain.step1.inner", 30, 90)
		s1.End()

		// child 2 with an event
		ctx2, s2 := tr.Start(ctx, "chain.step2")
		s2.AddEvent("step2.checkpoint", trace.WithAttributes(attribute.String("note", "halfway")))
		doSleepSpan(ctx2, tr, "chain.step2.inner", 40, 120)
		s2.End()

		sp.SetStatus(codes.Ok, "ok")
		return c.JSON(http.StatusOK, map[string]any{"result": "chain-ok"})
	})

	e.POST("/trace/manual", func(c echo.Context) error {
		tr := otel.Tracer("echo-otel-demo")
		ctx := context.Background()

		ctx, sp := tr.Start(ctx, "manual.trace",
			trace.WithAttributes(attribute.String("source", "manual_endpoint")),
		)
		defer sp.End()

		doSleepSpan(ctx, tr, "manual.db", 30, 120)
		doSleepSpan(ctx, tr, "manual.cache", 5, 25)
		sp.SetStatus(codes.Ok, "ok")

		return c.JSON(http.StatusOK, map[string]any{"ok": true})
	})

	e.GET("/assets/faqih", func(c echo.Context) error {
		imgPath := filepath.Join("resources", "assets", "faqih.png")
		c.Response().Header().Set(echo.HeaderContentType, "image/png")
		return c.File(imgPath)
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
		otlptracegrpc.WithInsecure(),
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

func doSleepSpan(ctx context.Context, tr trace.Tracer, name string, minMs, maxMs int, attrs ...attribute.KeyValue) {
	_, sp := tr.Start(ctx, name, trace.WithAttributes(attrs...))
	defer sp.End()

	sleepMs := rand.Intn(maxMs-minMs+1) + minMs
	sp.AddEvent("sleep.start", trace.WithAttributes(attribute.Int("sleep_ms", sleepMs)))
	time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	sp.AddEvent("sleep.end")
	sp.SetAttributes(attribute.Int("sleep_ms", sleepMs))
}

func httpCallSpan(ctx context.Context, tr trace.Tracer, url string) (int, error) {
	ctx, sp := tr.Start(ctx, "http.client",
		trace.WithAttributes(
			attribute.String("http.url", url),
			attribute.String("http.method", http.MethodGet),
		),
	)
	defer sp.End()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		sp.RecordError(err)
		sp.SetStatus(codes.Error, "new request failed")
		return 0, err
	}

	// Propagate trace context to downstream (important for distributed trace demo)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	dur := time.Since(start)

	if err != nil {
		sp.RecordError(err)
		sp.SetStatus(codes.Error, "http call failed")
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	sp.SetAttributes(
		attribute.Int("http.status_code", resp.StatusCode),
		attribute.Int64("http.duration_ms", dur.Milliseconds()),
	)

	if resp.StatusCode >= 500 {
		sp.SetStatus(codes.Error, "server error")
	} else {
		sp.SetStatus(codes.Ok, "ok")
	}

	return resp.StatusCode, nil
}

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"khaos" // root package

	_ "github.com/lib/pq"
)

type config struct {
	dbHost                 string
	dbPort                 int
	dbUser                 string
	dbPassword             string
	dbName                 string
	dbSSLMode              string

	dbMaxOpenConns         int
	dbMaxIdleConns         int
	dbConnMaxLifetime      time.Duration

	listenAddr             string
	workerCount            int
	batchTimeout           time.Duration
}

func loadConfig() (config, error) {
	cfg := config{
		dbHost:     getEnv("KHAOS_DB_HOST", "localhost"),
		dbPort:     getEnvInt("KHAOS_DB_PORT", 5432),
		dbUser:     getEnv("KHAOS_DB_USER", "postgres"),
		dbPassword: os.Getenv("KHAOS_DB_PASSWORD"), // intentionally no default
		dbName:     getEnv("KHAOS_DB_NAME", "khaos"),
		dbSSLMode:  getEnv("KHAOS_DB_SSLMODE", "disable"),

		dbMaxOpenConns:    getEnvInt("KHAOS_DB_MAX_OPEN_CONNS", 25),
		dbMaxIdleConns:    getEnvInt("KHAOS_DB_MAX_IDLE_CONNS", 25),
		dbConnMaxLifetime: getEnvDuration("KHAOS_DB_CONN_MAX_LIFETIME", 5*time.Minute),

		listenAddr:   getEnv("KHAOS_LISTEN_ADDR", ":8080"),
		workerCount:  getEnvInt("KHAOS_WORKER_COUNT", 8),
		batchTimeout: getEnvDuration("KHAOS_BATCH_TIMEOUT", 5*time.Second),
	}

	if cfg.dbPassword == "" {
		return config{}, fmt.Errorf("KHAOS_DB_PASSWORD environment variable is required and was not set")
	}

	return cfg, nil
}

func (c config) dsn() string{
	escape := func(s string) string{
		s = strings.ReplaceAll(s, `\`, `\\`)
		s = strings.ReplaceAll(s, `'`, `\'`)
		return "'" + s + "'"
	}
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		escape(c.dbHost), c.dbPort, escape(c.dbUser), escape(c.dbPassword), escape(c.dbName), escape(c.dbSSLMode),
	)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("warning: invalid integer for %s=%q, using default %d", key, v, fallback)
		return fallback
	}
	return n
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("warning: invalid duration for %s=%q, using default %s", key, v, fallback)
		return fallback
	}
	return d
}

func main() {
	if err := run(); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("configuratin error: %w", err)
	}

	db, err := sql.Open("postgres", cfg.dsn())
	if err != nil {
		return fmt.Errorf("opening database handle: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(cfg.dbMaxOpenConns)
	db.SetMaxIdleConns(cfg.dbMaxIdleConns)
	db.SetConnMaxLifetime(cfg.dbConnMaxLifetime)

	if err := waitForDB(db, 30*time.Second); err != nil {
		return fmt.Errorf("database not reachable: %w", err)
	}
	log.Println("connected to PostgreSQL successfully")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	setupCtx, cancelSetup := context.WithTimeout(ctx, 10*time.Second)
	defer cancelSetup()

	storage, err := khaos.NewPostgresStorageEngine(setupCtx, db)
	if err != nil {
		return fmt.Errorf("initializing storage engine: %w", err)
	}

	srv := khaos.NewServer(khaos.ServerConfig{
		Addr:         cfg.listenAddr,
		WorkerCount:  cfg.workerCount,
		BatchTimeout: cfg.batchTimeout,
	}, storage)

	log.Printf("khaos API gateway listening on %s", cfg.listenAddr)
	if err := srv.Run(ctx); err != nil {
		return fmt.Errorf("server exited with error: %w", err)
	}
	return nil
}

func waitForDB(db *sql.DB, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		pingCtx, cancelPing := context.WithTimeout(ctx, 2*time.Second)
		lastErr = db.PingContext(pingCtx)
		cancelPing()
		if lastErr == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("giving up after %s, last error: %w", timeout, lastErr)
		case <-ticker.C:
		}
	}
}
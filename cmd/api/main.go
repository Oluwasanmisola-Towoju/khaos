package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"khaos" // root package
	_ "github.com/lib/pq"
)

func main() {
	// connect to postgres (You can updae with your own actual DB credentials)
	dsn := "postgresql://postgres:Marvel04life%23@localhost:5432/Khaos_database?sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping DB: %v", err)
	}
	log.Println("Connected to PostgreSQL successfully.")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// initialize postgres storage engine
	storage, err := khaos.NewPostgresStorageEngine(ctx, db)
	if err != nil {
		log.Fatalf("Failed to initialize storage engine: %v", err)
	}

	// configure and Start the Server
	cfg := khaos.ServerConfig{
		Addr:         ":8080",
		WorkerCount:  8,
		BatchTimeout: 5 * time.Second,
	}
	
	srv := khaos.NewServer(cfg, storage)
	
	log.Printf("Khaos API Gateway listening on %s", cfg.Addr)
	if err := srv.Run(ctx); err != nil {
		log.Fatalf("Server exited with error: %v", err)
	}
}
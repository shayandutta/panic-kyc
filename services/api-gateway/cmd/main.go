package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"kyc-platform/services/api-gateway/internal/httpapi"
	"kyc-platform/shared/env"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	addr := env.GetString("HTTP_ADDR", ":8080")

	server := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewRouter(),
		ReadHeaderTimeout: 5 * time.Second, // slow-header clients can't hold connections forever
	}

	go func() {
		log.Printf("api gateway listening on %s", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http server failed: %v", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Println("shutting down api gateway")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server.Shutdown(shutdownCtx)
}

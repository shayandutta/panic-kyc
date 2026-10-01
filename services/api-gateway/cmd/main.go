package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"kyc-platform/services/api-gateway/internal/auth"
	"kyc-platform/services/api-gateway/internal/httpapi"
	"kyc-platform/services/api-gateway/internal/ratelimit"
	"kyc-platform/shared/env"
	bulkpb "kyc-platform/shared/proto/bulk"
	pb "kyc-platform/shared/proto/verification"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var (
		addr            = env.GetString("HTTP_ADDR", ":8080")
		clientsFile     = env.GetString("CLIENTS_FILE", "deploy/dev/clients.json")
		redisAddr       = env.GetString("REDIS_ADDR", "localhost:6379")
		verificationURL = env.GetString("VERIFICATION_SERVICE_ADDR", "localhost:9093")
		bulkURL         = env.GetString("BULK_SERVICE_ADDR", "localhost:9095")
	)

	keys, err := auth.LoadKeyStore(clientsFile)
	if err != nil {
		log.Fatal(err)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()

	// One gRPC connection, created once and shared by all requests. It's safe
	// for concurrent use, multiplexes calls over HTTP/2 and reconnects by itself.
	verificationConn, err := grpc.NewClient(verificationURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("create verification client: %v", err)
	}
	defer verificationConn.Close()

	bulkConn, err := grpc.NewClient(bulkURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("create bulk client: %v", err)
	}
	defer bulkConn.Close()

	server := &http.Server{
		Addr: addr,
		Handler: httpapi.NewRouter(httpapi.Deps{
			Keys:          keys,
			Limiter:       ratelimit.New(redisClient),
			Verifications: pb.NewVerificationServiceClient(verificationConn),
			Bulk:          bulkpb.NewBulkServiceClient(bulkConn),
		}),
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

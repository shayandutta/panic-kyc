package main

import (
	"context"
	"log"
	"net"
	"os/signal"
	"syscall"
	"time"

	"kyc-platform/services/bulk-service/internal/grpcapi"
	"kyc-platform/services/bulk-service/internal/processor"
	"kyc-platform/services/bulk-service/internal/repository"
	"kyc-platform/shared/db"
	"kyc-platform/shared/env"
	pb "kyc-platform/shared/proto/verification"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var (
		grpcAddr        = env.GetString("GRPC_ADDR", ":9095")
		mongoURI        = env.GetString("MONGODB_URI", "mongodb://localhost:27018")
		mongoDB         = env.GetString("MONGODB_DATABASE", "kyc")
		verificationURL = env.GetString("VERIFICATION_SERVICE_ADDR", "localhost:9093")
		cfg             = processor.Config{
			MinWorkers:  env.GetInt("BULK_MIN_WORKERS", 2),
			MaxWorkers:  env.GetInt("BULK_MAX_WORKERS", 20),
			QueueSize:   env.GetInt("BULK_QUEUE_SIZE", 1000),
			ItemTimeout: env.GetDuration("BULK_ITEM_TIMEOUT", 5*time.Second),
		}
	)

	mongoClient, err := db.ConnectMongo(ctx, mongoURI)
	if err != nil {
		log.Fatal(err)
	}
	defer mongoClient.Disconnect(context.Background())

	conn, err := grpc.NewClient(verificationURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("create verification client: %v", err)
	}
	defer conn.Close()

	proc := processor.New(repository.NewMongoRepository(mongoClient.Database(mongoDB)), pb.NewVerificationServiceClient(conn), cfg)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("listen on %s: %v", grpcAddr, err)
	}
	server := grpc.NewServer()
	grpcapi.Register(server, proc)
	reflection.Register(server)

	go func() {
		log.Printf("bulk service listening on %s (workers %d-%d)", grpcAddr, cfg.MinWorkers, cfg.MaxWorkers)
		if err := server.Serve(lis); err != nil {
			log.Printf("grpc server stopped: %v", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Println("shutting down bulk service")
	server.GracefulStop() // no new jobs
	proc.Close()          // finish what's queued
}

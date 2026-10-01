// Package grpcapi exposes bulk jobs over gRPC.
package grpcapi

import (
	"context"
	"errors"
	"time"

	"kyc-platform/services/bulk-service/internal/domain"
	"kyc-platform/services/bulk-service/internal/processor"
	pb "kyc-platform/shared/proto/bulk"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	pb.UnimplementedBulkServiceServer
	proc *processor.Processor
}

func Register(server *grpc.Server, proc *processor.Processor) {
	pb.RegisterBulkServiceServer(server, &Handler{proc: proc})
}

func (h *Handler) CreateJob(ctx context.Context, req *pb.CreateJobRequest) (*pb.Job, error) {
	if req.GetClientId() == "" {
		return nil, status.Error(codes.InvalidArgument, "client_id is required")
	}

	items := make([]domain.Item, len(req.GetItems()))
	for i, it := range req.GetItems() {
		items[i] = domain.Item{PAN: it.GetPan(), Name: it.GetName(), ReferenceID: it.GetReferenceId()}
	}

	job, err := h.proc.CreateJob(ctx, req.GetClientId(), items)
	if errors.Is(err, processor.ErrBadJob) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "internal error")
	}
	return toProto(job), nil
}

func (h *Handler) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.Job, error) {
	job, err := h.proc.GetJob(ctx, req.GetClientId(), req.GetId())
	if errors.Is(err, domain.ErrNotFound) {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "internal error")
	}
	return toProto(job), nil
}

func toProto(j *domain.Job) *pb.Job {
	st := map[domain.Status]pb.JobStatus{
		domain.StatusQueued:    pb.JobStatus_JOB_STATUS_QUEUED,
		domain.StatusRunning:   pb.JobStatus_JOB_STATUS_RUNNING,
		domain.StatusCompleted: pb.JobStatus_JOB_STATUS_COMPLETED,
	}[j.Status]
	return &pb.Job{
		Id:        j.ID,
		ClientId:  j.ClientID,
		Status:    st,
		Total:     int32(j.Total),
		Processed: int32(j.Processed),
		Valid:     int32(j.Valid),
		Invalid:   int32(j.Invalid),
		Failed:    int32(j.Failed),
		CreatedAt: j.CreatedAt.Format(time.RFC3339),
	}
}

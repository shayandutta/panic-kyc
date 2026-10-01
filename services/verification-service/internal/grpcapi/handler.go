// Package grpcapi exposes the verification service over gRPC.
package grpcapi

import (
	"context"
	"errors"
	"time"

	"kyc-platform/services/verification-service/internal/domain"
	"kyc-platform/services/verification-service/internal/service"
	pb "kyc-platform/shared/proto/verification"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	pb.UnimplementedVerificationServiceServer
	svc *service.Service
}

func Register(server *grpc.Server, svc *service.Service) {
	pb.RegisterVerificationServiceServer(server, &Handler{svc: svc})
}

func (h *Handler) VerifyPAN(ctx context.Context, req *pb.VerifyPANRequest) (*pb.Verification, error) {
	if req.GetClientId() == "" {
		return nil, status.Error(codes.InvalidArgument, "client_id is required")
	}

	v, err := h.svc.VerifyPAN(ctx, service.VerifyRequest{
		ClientID:    req.GetClientId(),
		PAN:         req.GetPan(),
		Name:        req.GetName(),
		ReferenceID: req.GetReferenceId(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return toProto(v), nil
}

func (h *Handler) GetVerification(ctx context.Context, req *pb.GetVerificationRequest) (*pb.Verification, error) {
	v, err := h.svc.GetVerification(ctx, req.GetClientId(), req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return toProto(v), nil
}

// toStatus maps business errors to gRPC status codes, so the gateway can
// turn them into the right HTTP status and API error code.
func toStatus(err error) error {
	switch {
	case errors.Is(err, service.ErrInvalidPAN):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, service.ErrSourceUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

func toProto(v *domain.Verification) *pb.Verification {
	st := pb.VerificationStatus_VERIFICATION_STATUS_INVALID
	if v.Status == domain.StatusValid {
		st = pb.VerificationStatus_VERIFICATION_STATUS_VALID
	}
	return &pb.Verification{
		Id:          v.ID,
		ClientId:    v.ClientID,
		ReferenceId: v.ReferenceID,
		PanMasked:   v.PANMasked,
		Status:      st,
		NameMatch:   v.NameMatch,
		Source:      v.Source,
		CreatedAt:   v.CreatedAt.Format(time.RFC3339),
	}
}

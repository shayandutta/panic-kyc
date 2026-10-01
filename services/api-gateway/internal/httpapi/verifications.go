package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"kyc-platform/services/api-gateway/internal/auth"
	"kyc-platform/shared/contracts"
	pb "kyc-platform/shared/proto/verification"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// upstreamTimeout is the most we wait for internal services on the
// synchronous path. It's passed down as a gRPC deadline.
const upstreamTimeout = 3 * time.Second

type verifyPANRequest struct {
	PAN         string `json:"pan"`
	Name        string `json:"name"`
	ReferenceID string `json:"reference_id"`
}

type verificationResponse struct {
	ID          string `json:"id"`
	ReferenceID string `json:"reference_id,omitempty"`
	PANMasked   string `json:"pan_masked"`
	Status      string `json:"status"`
	NameMatch   bool   `json:"name_match"`
	Source      string `json:"source"`
	CreatedAt   string `json:"created_at"`
}

type verificationHandlers struct {
	client pb.VerificationServiceClient
}

func (h *verificationHandlers) verifyPAN(w http.ResponseWriter, r *http.Request) {
	client, _ := auth.ClientFrom(r.Context())

	var req verifyPANRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, contracts.ErrCodeBadRequest, "body must be valid JSON")
		return
	}
	if req.PAN == "" {
		writeError(w, http.StatusBadRequest, contracts.ErrCodeBadRequest, "pan is required")
		return
	}
	// The standard Idempotency-Key header works too, if the body has no reference_id.
	if req.ReferenceID == "" {
		req.ReferenceID = r.Header.Get("Idempotency-Key")
	}

	ctx, cancel := grpcContext(r)
	defer cancel()

	v, err := h.client.VerifyPAN(ctx, &pb.VerifyPANRequest{
		ClientId:    client.ID, // from the API key, never from the body
		Pan:         req.PAN,
		Name:        req.Name,
		ReferenceId: req.ReferenceID,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toResponse(v))
}

func (h *verificationHandlers) getVerification(w http.ResponseWriter, r *http.Request) {
	client, _ := auth.ClientFrom(r.Context())

	ctx, cancel := grpcContext(r)
	defer cancel()

	v, err := h.client.GetVerification(ctx, &pb.GetVerificationRequest{
		ClientId: client.ID,
		Id:       r.PathValue("id"),
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toResponse(v))
}

// grpcContext adds a deadline and forwards the request ID to internal services.
func grpcContext(r *http.Request) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(r.Context(), upstreamTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", RequestIDFrom(r.Context()))
	return ctx, cancel
}

// writeGRPCError turns internal gRPC codes into public HTTP statuses and
// stable error codes. Internal error details are never sent to clients.
func writeGRPCError(w http.ResponseWriter, err error) {
	st, _ := status.FromError(err)
	switch st.Code() {
	case codes.InvalidArgument:
		writeError(w, http.StatusBadRequest, contracts.ErrCodeInvalidPAN, st.Message())
	case codes.NotFound:
		writeError(w, http.StatusNotFound, contracts.ErrCodeNotFound, "verification not found")
	case codes.Unavailable:
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, contracts.ErrCodeSourceUnavailable, "no data source is available right now, retry later")
	case codes.DeadlineExceeded:
		writeError(w, http.StatusGatewayTimeout, contracts.ErrCodeSourceUnavailable, "verification timed out, retry later")
	default:
		writeError(w, http.StatusInternalServerError, contracts.ErrCodeInternal, "internal error")
	}
}

func toResponse(v *pb.Verification) verificationResponse {
	statusText := "INVALID"
	if v.GetStatus() == pb.VerificationStatus_VERIFICATION_STATUS_VALID {
		statusText = "VALID"
	}
	return verificationResponse{
		ID:          v.GetId(),
		ReferenceID: v.GetReferenceId(),
		PANMasked:   v.GetPanMasked(),
		Status:      statusText,
		NameMatch:   v.GetNameMatch(),
		Source:      v.GetSource(),
		CreatedAt:   v.GetCreatedAt(),
	}
}

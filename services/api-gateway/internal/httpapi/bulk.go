package httpapi

import (
	"encoding/json"
	"net/http"

	"kyc-platform/services/api-gateway/internal/auth"
	"kyc-platform/shared/contracts"
	pb "kyc-platform/shared/proto/bulk"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type bulkJobRequest struct {
	Items []struct {
		PAN         string `json:"pan"`
		Name        string `json:"name"`
		ReferenceID string `json:"reference_id"`
	} `json:"items"`
}

type bulkJobResponse struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	Total     int32  `json:"total"`
	Processed int32  `json:"processed"`
	Valid     int32  `json:"valid"`
	Invalid   int32  `json:"invalid"`
	Failed    int32  `json:"failed"`
	CreatedAt string `json:"created_at"`
}

type bulkHandlers struct {
	client pb.BulkServiceClient
}

// createJob answers 202 Accepted: the work is queued, not done. The client
// polls GET /v1/bulk-jobs/{id}, and each finished row also triggers a webhook.
func (h *bulkHandlers) createJob(w http.ResponseWriter, r *http.Request) {
	client, _ := auth.ClientFrom(r.Context())

	var req bulkJobRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 5<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, contracts.ErrCodeBadRequest, "body must be valid JSON, at most 5 MB")
		return
	}

	items := make([]*pb.BulkItem, len(req.Items))
	for i, it := range req.Items {
		items[i] = &pb.BulkItem{Pan: it.PAN, Name: it.Name, ReferenceId: it.ReferenceID}
	}

	ctx, cancel := grpcContext(r)
	defer cancel()

	job, err := h.client.CreateJob(ctx, &pb.CreateJobRequest{ClientId: client.ID, Items: items})
	if err != nil {
		writeBulkError(w, err)
		return
	}
	w.Header().Set("Location", "/v1/bulk-jobs/"+job.GetId())
	writeJSON(w, http.StatusAccepted, toBulkResponse(job))
}

func (h *bulkHandlers) getJob(w http.ResponseWriter, r *http.Request) {
	client, _ := auth.ClientFrom(r.Context())

	ctx, cancel := grpcContext(r)
	defer cancel()

	job, err := h.client.GetJob(ctx, &pb.GetJobRequest{ClientId: client.ID, Id: r.PathValue("id")})
	if err != nil {
		writeBulkError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toBulkResponse(job))
}

func writeBulkError(w http.ResponseWriter, err error) {
	st, _ := status.FromError(err)
	switch st.Code() {
	case codes.InvalidArgument:
		writeError(w, http.StatusBadRequest, contracts.ErrCodeBadRequest, st.Message())
	case codes.NotFound:
		writeError(w, http.StatusNotFound, contracts.ErrCodeNotFound, "bulk job not found")
	default:
		writeGRPCError(w, err)
	}
}

func toBulkResponse(j *pb.Job) bulkJobResponse {
	statusText := map[pb.JobStatus]string{
		pb.JobStatus_JOB_STATUS_QUEUED:    "QUEUED",
		pb.JobStatus_JOB_STATUS_RUNNING:   "RUNNING",
		pb.JobStatus_JOB_STATUS_COMPLETED: "COMPLETED",
	}[j.GetStatus()]
	return bulkJobResponse{
		ID:        j.GetId(),
		Status:    statusText,
		Total:     j.GetTotal(),
		Processed: j.GetProcessed(),
		Valid:     j.GetValid(),
		Invalid:   j.GetInvalid(),
		Failed:    j.GetFailed(),
		CreatedAt: j.GetCreatedAt(),
	}
}

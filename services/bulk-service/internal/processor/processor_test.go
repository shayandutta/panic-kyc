package processor

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"kyc-platform/services/bulk-service/internal/domain"
	"kyc-platform/services/bulk-service/internal/repository"
	pb "kyc-platform/shared/proto/verification"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeVerifier answers by PAN: ends in X = invalid, ends in Z = provider down.
type fakeVerifier struct {
	mu   sync.Mutex
	refs []string
}

func (f *fakeVerifier) VerifyPAN(ctx context.Context, in *pb.VerifyPANRequest, _ ...grpc.CallOption) (*pb.Verification, error) {
	f.mu.Lock()
	f.refs = append(f.refs, in.GetReferenceId())
	f.mu.Unlock()
	time.Sleep(time.Millisecond)
	switch {
	case strings.HasSuffix(in.GetPan(), "Z"):
		return nil, status.Error(codes.Unavailable, "down")
	case strings.HasSuffix(in.GetPan(), "X"):
		return &pb.Verification{Status: pb.VerificationStatus_VERIFICATION_STATUS_INVALID}, nil
	default:
		return &pb.Verification{Status: pb.VerificationStatus_VERIFICATION_STATUS_VALID}, nil
	}
}

func (f *fakeVerifier) GetVerification(ctx context.Context, in *pb.GetVerificationRequest, _ ...grpc.CallOption) (*pb.Verification, error) {
	return nil, nil
}

func waitForCompletion(t *testing.T, p *Processor, clientID, id string) *domain.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, err := p.GetJob(context.Background(), clientID, id)
		if err == nil && j.Status == domain.StatusCompleted {
			return j
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job did not complete in time")
	return nil
}

func newTestProcessor(v *fakeVerifier) *Processor {
	return New(repository.NewInmemRepository(), v, Config{MinWorkers: 1, MaxWorkers: 8, QueueSize: 50, ItemTimeout: time.Second})
}

func TestJobCountsEveryOutcome(t *testing.T) {
	v := &fakeVerifier{}
	p := newTestProcessor(v)
	defer p.Close()

	var items []domain.Item
	for i := 0; i < 300; i++ {
		pan := "ABCDE1234F"
		switch i % 3 {
		case 1:
			pan = "ABCDE1234X"
		case 2:
			pan = "ABCDE1234Z"
		}
		items = append(items, domain.Item{PAN: pan})
	}

	job, err := p.CreateJob(context.Background(), "bank-1", items)
	if err != nil {
		t.Fatal(err)
	}
	done := waitForCompletion(t, p, "bank-1", job.ID)
	if done.Processed != 300 || done.Valid != 100 || done.Invalid != 100 || done.Failed != 100 {
		t.Errorf("unexpected counts %+v", done)
	}
}

func TestEveryItemGetsADeterministicReference(t *testing.T) {
	v := &fakeVerifier{}
	p := newTestProcessor(v)
	defer p.Close()

	job, _ := p.CreateJob(context.Background(), "bank-1", []domain.Item{{PAN: "ABCDE1234F"}, {PAN: "ABCDE1234F", ReferenceID: "client-ref"}})
	waitForCompletion(t, p, "bank-1", job.ID)

	refs := strings.Join(v.refs, ",")
	if !strings.Contains(refs, "bulk:"+job.ID+":0") || !strings.Contains(refs, "client-ref") {
		t.Errorf("references = %s", refs)
	}
}

func TestRejectsEmptyAndOversizedJobs(t *testing.T) {
	p := newTestProcessor(&fakeVerifier{})
	defer p.Close()
	if _, err := p.CreateJob(context.Background(), "bank-1", nil); err != ErrBadJob {
		t.Errorf("empty job: err = %v", err)
	}
	if _, err := p.CreateJob(context.Background(), "bank-1", make([]domain.Item, MaxItems+1)); err != ErrBadJob {
		t.Errorf("oversized job: err = %v", err)
	}
}

func TestClientsCannotSeeEachOthersJobs(t *testing.T) {
	p := newTestProcessor(&fakeVerifier{})
	defer p.Close()
	job, _ := p.CreateJob(context.Background(), "bank-1", []domain.Item{{PAN: "ABCDE1234F"}})
	if _, err := p.GetJob(context.Background(), "lender-2", job.ID); err != domain.ErrNotFound {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

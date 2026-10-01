package webhooksig

import (
	"errors"
	"testing"
	"time"
)

var (
	body = []byte(`{"event_id":"e1","status":"VALID"}`)
	now  = time.Unix(1_800_000_000, 0)
)

func TestSignAndVerify(t *testing.T) {
	h := Sign("whsec_1", body, now)
	if err := Verify("whsec_1", h, body, now.Add(time.Minute), 5*time.Minute); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
}

func TestTamperedBodyFails(t *testing.T) {
	h := Sign("whsec_1", body, now)
	tampered := []byte(`{"event_id":"e1","status":"INVALID"}`)
	if err := Verify("whsec_1", h, tampered, now, 5*time.Minute); !errors.Is(err, ErrMismatch) {
		t.Errorf("err = %v, want ErrMismatch", err)
	}
}

func TestWrongSecretFails(t *testing.T) {
	h := Sign("whsec_1", body, now)
	if err := Verify("whsec_other", h, body, now, 5*time.Minute); !errors.Is(err, ErrMismatch) {
		t.Errorf("err = %v, want ErrMismatch", err)
	}
}

func TestReplayedOldMessageFails(t *testing.T) {
	h := Sign("whsec_1", body, now)
	if err := Verify("whsec_1", h, body, now.Add(10*time.Minute), 5*time.Minute); !errors.Is(err, ErrTooOld) {
		t.Errorf("err = %v, want ErrTooOld", err)
	}
}

func TestMalformedHeader(t *testing.T) {
	for _, h := range []string{"", "garbage", "t=abc,v1=x", "v1=onlysig"} {
		if err := Verify("s", h, body, now, time.Minute); !errors.Is(err, ErrMalformed) {
			t.Errorf("header %q: err = %v, want ErrMalformed", h, err)
		}
	}
}

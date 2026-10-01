package source

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPSourceReturnsResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"exists":true,"name":"Asha Verma"}`))
	}))
	defer srv.Close()

	res, err := NewHTTPSource("a", srv.URL, time.Second).LookupPAN(context.Background(), "PQRST6789K")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Exists || res.Name != "Asha Verma" {
		t.Errorf("unexpected result %+v", res)
	}
}

func TestHTTPSourceTreatsNon200AsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	if _, err := NewHTTPSource("a", srv.URL, time.Second).LookupPAN(context.Background(), "X"); err == nil {
		t.Error("expected error on 503")
	}
}

func TestHTTPSourceTimesOut(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()

	start := time.Now()
	_, err := NewHTTPSource("a", srv.URL, 50*time.Millisecond).LookupPAN(context.Background(), "X")
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if time.Since(start) > 150*time.Millisecond {
		t.Error("timeout was not enforced")
	}
}

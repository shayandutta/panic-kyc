package auth

import (
	"errors"
	"testing"
)

func TestAuthenticate(t *testing.T) {
	store := NewKeyStore([]Client{{ID: "bank-1", KeySHA256: HashKey("live_secret")}})

	c, err := store.Authenticate("live_secret")
	if err != nil || c.ID != "bank-1" {
		t.Fatalf("c=%+v err=%v", c, err)
	}
	if _, err := store.Authenticate("live_wrong"); !errors.Is(err, ErrUnauthorized) {
		t.Error("wrong key must be rejected")
	}
	if _, err := store.Authenticate(""); !errors.Is(err, ErrUnauthorized) {
		t.Error("empty key must be rejected")
	}
}

func TestKeyFromHeader(t *testing.T) {
	if KeyFromHeader("Bearer abc", "") != "abc" {
		t.Error("bearer token not read")
	}
	if KeyFromHeader("", "xyz") != "xyz" {
		t.Error("X-API-Key not read")
	}
}

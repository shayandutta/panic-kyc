// Package webhooksig signs and verifies webhook payloads, the same way
// Stripe does: an HMAC over the timestamp and the body.
//
// Header format:  X-KYC-Signature: t=1727900000,v1=<hex hmac>
package webhooksig

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const Header = "X-KYC-Signature"

var (
	ErrMalformed = errors.New("malformed signature header")
	ErrTooOld    = errors.New("signature timestamp outside tolerance")
	ErrMismatch  = errors.New("signature does not match")
)

// Sign returns the header value for body, signed with the client's secret.
// The timestamp is part of what's signed, so an attacker can't take an old
// valid message and resend it later (a replay attack).
func Sign(secret string, body []byte, at time.Time) string {
	ts := at.Unix()
	return fmt.Sprintf("t=%d,v1=%s", ts, compute(secret, ts, body))
}

// Verify checks a header against body. Receivers should reject anything
// older than tolerance (5 minutes is typical).
func Verify(secret, header string, body []byte, now time.Time, tolerance time.Duration) error {
	var ts int64
	var sig string
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			return ErrMalformed
		}
		switch k {
		case "t":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return ErrMalformed
			}
			ts = n
		case "v1":
			sig = v
		}
	}
	if ts == 0 || sig == "" {
		return ErrMalformed
	}

	age := now.Sub(time.Unix(ts, 0))
	if age > tolerance || age < -tolerance {
		return ErrTooOld
	}

	// hmac.Equal compares in constant time, so response timing doesn't
	// leak how many characters of a forged signature were right.
	if !hmac.Equal([]byte(sig), []byte(compute(secret, ts, body))) {
		return ErrMismatch
	}
	return nil
}

func compute(secret string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.", ts)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// Package pii holds helpers for handling personal data such as PAN numbers.
// Raw PII must never be logged or stored in plain text.
package pii

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

var panPattern = regexp.MustCompile(`^[A-Z]{5}[0-9]{4}[A-Z]$`)

// NormalizePAN uppercases and trims a PAN.
func NormalizePAN(pan string) string {
	return strings.ToUpper(strings.TrimSpace(pan))
}

// IsValidPAN checks the PAN format: 5 letters, 4 digits, 1 letter.
func IsValidPAN(pan string) bool {
	return panPattern.MatchString(pan)
}

// MaskPAN keeps the first two and last two characters: AB******4F.
func MaskPAN(pan string) string {
	if len(pan) < 4 {
		return strings.Repeat("*", len(pan))
	}
	return pan[:2] + strings.Repeat("*", len(pan)-4) + pan[len(pan)-2:]
}

// Fingerprint returns a keyed hash of a value. It lets us look up or cache
// by PAN without storing the PAN itself.
func Fingerprint(value, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))
}

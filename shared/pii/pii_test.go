package pii

import "testing"

func TestIsValidPAN(t *testing.T) {
	cases := map[string]bool{
		"ABCDE1234F": true,
		"abcde1234f": false, // must be normalized first
		"ABCD1234F":  false,
		"ABCDE12345": false,
		"":           false,
	}
	for pan, want := range cases {
		if got := IsValidPAN(pan); got != want {
			t.Errorf("IsValidPAN(%q) = %v, want %v", pan, got, want)
		}
	}
}

func TestMaskPAN(t *testing.T) {
	if got := MaskPAN("ABCDE1234F"); got != "AB******4F" {
		t.Errorf("MaskPAN = %q", got)
	}
}

func TestFingerprintIsStableAndKeyed(t *testing.T) {
	a := Fingerprint("ABCDE1234F", "k1")
	if a != Fingerprint("ABCDE1234F", "k1") {
		t.Error("fingerprint not stable")
	}
	if a == Fingerprint("ABCDE1234F", "k2") {
		t.Error("fingerprint ignores the secret")
	}
}

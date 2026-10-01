package domain

import "testing"

func TestNormalizeName(t *testing.T) {
	cases := []struct{ a, b string }{
		{"Shayan Dutta", "shayan dutta"},
		{"DUTTA  Shayan", "Shayan Dutta"},
		{"  Asha Verma ", "asha verma"},
	}
	for _, c := range cases {
		if NormalizeName(c.a) != NormalizeName(c.b) {
			t.Errorf("%q and %q should match", c.a, c.b)
		}
	}
	if NormalizeName("Asha Verma") == NormalizeName("Asha Varma") {
		t.Error("different names must not match")
	}
}

package server

import (
	"testing"
	"time"
)

func TestCursor_RoundTrip(t *testing.T) {
	c := cursor{SubmittedAt: time.Date(2026, 11, 24, 9, 0, 0, 123456000, time.UTC), ApplicationID: "a1"}

	got, err := decodeCursor(encodeCursor(c))

	if err != nil || !got.SubmittedAt.Equal(c.SubmittedAt) || got.ApplicationID != c.ApplicationID {
		t.Fatalf("round trip = (%v, %v), want %v", got, err, c)
	}
}

func TestCursor_RejectsGarbage(t *testing.T) {
	for _, token := range []string{"not-a-token", "e30", encodeCursor(cursor{ApplicationID: "a1"})} {
		if _, err := decodeCursor(token); err == nil {
			t.Errorf("decodeCursor(%q) succeeded, want error", token)
		}
	}
}

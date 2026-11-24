package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"
)

// cursor is the position after the last row of a page. Clients get it as an
// opaque token; keyset pagination keeps pages stable while rows are added.
type cursor struct {
	SubmittedAt   time.Time `json:"t"`
	ApplicationID string    `json:"id"`
}

func encodeCursor(c cursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(token string) (cursor, error) {
	var c cursor
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return c, err
	}
	if c.ApplicationID == "" || c.SubmittedAt.IsZero() {
		return c, errors.New("incomplete cursor")
	}
	return c, nil
}

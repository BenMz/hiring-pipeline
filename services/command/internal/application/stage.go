package application

import "fmt"

// Stage mirrors pipeline.v1.Stage; the numeric values match the proto enum.
type Stage int32

// Funnel stages in order. Hired and Rejected are terminal.
const (
	Unspecified Stage = iota
	Applied
	Screening
	Interview
	Offer
	Hired
	Rejected
)

var stageNames = map[Stage]string{
	Unspecified: "UNSPECIFIED",
	Applied:     "APPLIED",
	Screening:   "SCREENING",
	Interview:   "INTERVIEW",
	Offer:       "OFFER",
	Hired:       "HIRED",
	Rejected:    "REJECTED",
}

func (s Stage) String() string {
	if name, ok := stageNames[s]; ok {
		return name
	}
	return fmt.Sprintf("Stage(%d)", int32(s))
}

// next is the funnel: each open stage may only advance one step.
// Rejection is a separate command, allowed from any open stage.
var next = map[Stage]Stage{
	Applied:   Screening,
	Screening: Interview,
	Interview: Offer,
	Offer:     Hired,
}

// Terminal reports whether no further moves are possible.
func (s Stage) Terminal() bool { return s == Hired || s == Rejected }

func (s Stage) canMoveTo(to Stage) bool {
	n, ok := next[s]
	return ok && n == to
}

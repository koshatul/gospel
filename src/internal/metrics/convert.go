package metrics

import (
	"time"

	"golang.org/x/time/rate"
)

// RateToDuration converts a rate limit to its interval duration.
func RateToDuration(r rate.Limit) time.Duration {
	return time.Duration(
		(1.0 / r) * rate.Limit(time.Second),
	)
}

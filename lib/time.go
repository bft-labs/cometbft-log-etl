package lib

import "time"

func MustParseUtcTimestamp(ts string) time.Time {
	// Parse the timestamp string into a time.Time object
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		panic(err)
	}
	// Convert the time.Time object to Unix timestamp in seconds
	return t.UTC()
}

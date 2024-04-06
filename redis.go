// Redis-specific behavior and errors.
package redka

import (
	"strings"
)

// setRange overwrites part of the string according to Redis semantics.
func setRange(s string, offset int, value string) string {
	if offset < 0 {
		offset += len(s)

		if offset < 0 {
			offset = 0

		} else if offset > len(s) {
			offset = len(s)
		}
	}

	if offset > len(s) {
		return s + strings.Repeat("\x00", offset-len(s)) + value
	}
	if offset+len(value) > len(s) {
		return s[:offset] + value
	}
	return s[:offset] + value + s[offset+len(value):]
}

// rangeToSlice converts Redis range offsets to Go slice offsets.
func rangeToSlice(length, start, end int) (int, int) {
	if start < 0 {
		start += length
	}
	if start < 0 {
		start = 0
	} else if start > length {
		start = length
	}

	if end < 0 {
		end += length
	}
	end++

	if end <= 0 || end < start {
		// for some reason Redis does not return an empty string in this case
		return 0, 1
	}
	if end > length {
		end = length
	}

	return start, end
}

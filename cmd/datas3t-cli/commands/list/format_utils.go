package list

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
)

// FormatBytes formats bytes with a fixed number of decimal places for better visual comparison
func FormatBytes(bytes uint64, precision int) string {
	// Use humanize to get the base formatting
	humanized := humanize.Bytes(bytes)

	// Split into numeric and unit parts
	parts := strings.Fields(humanized)
	if len(parts) != 2 {
		return humanized // Return original if format is unexpected
	}

	// Parse the numeric part and format with fixed precision
	value, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return humanized // Return original if parsing fails
	}

	// Format with fixed precision
	formatted := fmt.Sprintf("%.*f %s", precision, value, parts[1])
	return formatted
}

// FormatBytesAsGB normalizes all sizes to gigabytes for easier comparison
func FormatBytesAsGB(bytes uint64, precision int) string {
	// Convert to GB with fixed precision
	gbValue := float64(bytes) / (1024 * 1024 * 1024)
	return fmt.Sprintf("%.*f GB", precision, gbValue)
}

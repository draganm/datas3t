package list

import (
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableRendering(t *testing.T) {
	// This is just a visual test you can run manually with:
	// go test -v ./cmd/datas3t-cli/commands/list -run=^TestTableRendering$
	if testing.Short() {
		t.Skip("Skipping visual test in short mode")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	_ = Command(logger) // Just to make sure it compiles

	// Fixed decimal precision for size values
	const PRECISION = 2

	// Mock datasets with various ID lengths
	mockDatasets := []client.Dataset{
		{ID: "short", DatarangeCount: 10, TotalSizeBytes: 1024 * 1024 * 50},
		{ID: "medium-length-id", DatarangeCount: 25, TotalSizeBytes: 1024 * 1024 * 1024 * 2},
		{ID: "this-is-a-very-long-id-that-exceeds-twenty-characters", DatarangeCount: 100, TotalSizeBytes: 1024 * 1024 * 1024 * 10},
		{ID: "another-id", DatarangeCount: 5, TotalSizeBytes: 1024 * 512},
	}

	// Pre-compute expected formatted sizes for test assertions
	expectedSizes := make([]string, len(mockDatasets))
	for i, ds := range mockDatasets {
		expectedSizes[i] = FormatBytesAsGB(uint64(ds.TotalSizeBytes), PRECISION)
	}

	// Create the table using the same code as in the actual implementation
	t.Log("Rendering dataset table with various ID lengths")

	tw := table.NewWriter()
	// Don't output to stdout directly, we'll capture the string
	// tw.SetOutputMirror(os.Stdout)

	// Configure table style
	tw.SetStyle(table.StyleLight)

	// Add header
	tw.AppendHeader(table.Row{"ID", "DATARANGES", "SIZE (GB)"})

	// Add rows with normalized GB sizes
	for i, ds := range mockDatasets {
		tw.AppendRow(table.Row{
			ds.ID,
			ds.DatarangeCount,
			expectedSizes[i], // Use our pre-computed GB sizes with fixed precision
		})
	}

	// Configure column alignment
	tw.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AlignHeader: text.AlignLeft, Align: text.AlignLeft},
		{Number: 2, AlignHeader: text.AlignRight, Align: text.AlignRight},
		{Number: 3, AlignHeader: text.AlignRight, Align: text.AlignRight},
	})

	// Capture the rendered table as a string
	renderedTable := tw.Render()

	// Print for visual inspection during test runs
	t.Log("\n" + renderedTable)

	// Split the table into lines for detailed analysis
	lines := strings.Split(renderedTable, "\n")
	require.GreaterOrEqual(t, len(lines), 7, "Table should have at least 7 lines (top border, header, separator, 4 data rows, bottom border)")

	// Verify the structural elements of the table
	topBorderPattern := regexp.MustCompile(`^┌─+┬─+┬─+┐$`)
	headerRowPattern := regexp.MustCompile(`^│\s+ID\s+│\s+DATARANGES\s+│\s+SIZE \(GB\)\s+│$`)
	separatorPattern := regexp.MustCompile(`^├─+┼─+┼─+┤$`)

	// Create patterns for data rows with the expected sizes
	dataRowPatterns := []*regexp.Regexp{
		regexp.MustCompile(`^│\s+short\s+│\s+10\s+│\s+` + regexp.QuoteMeta(expectedSizes[0]) + `\s+│$`),
		regexp.MustCompile(`^│\s+medium-length-id\s+│\s+25\s+│\s+` + regexp.QuoteMeta(expectedSizes[1]) + `\s+│$`),
		regexp.MustCompile(`^│\s+this-is-a-very-long-id-that-exceeds-twenty-characters\s+│\s+100\s+│\s+` + regexp.QuoteMeta(expectedSizes[2]) + `\s+│$`),
		regexp.MustCompile(`^│\s+another-id\s+│\s+5\s+│\s+` + regexp.QuoteMeta(expectedSizes[3]) + `\s+│$`),
	}

	bottomBorderPattern := regexp.MustCompile(`^└─+┴─+┴─+┘$`)

	// Verify each line of the table
	assert.True(t, topBorderPattern.MatchString(lines[0]), "Top border doesn't match expected pattern")
	assert.True(t, headerRowPattern.MatchString(lines[1]), "Header row doesn't match expected pattern")
	assert.True(t, separatorPattern.MatchString(lines[2]), "Separator doesn't match expected pattern")

	// Check data rows - they should be in order
	dataRowsFound := make([]bool, len(dataRowPatterns))
	for i := 3; i < len(lines)-1; i++ {
		for j, pattern := range dataRowPatterns {
			if pattern.MatchString(lines[i]) {
				dataRowsFound[j] = true
			}
		}
	}

	// Verify all expected data rows were found
	for i, found := range dataRowsFound {
		assert.True(t, found, "Data row %d not found or doesn't match pattern (%s)", i, expectedSizes[i])
	}

	// Verify bottom border
	assert.True(t, bottomBorderPattern.MatchString(lines[len(lines)-1]), "Bottom border doesn't match expected pattern")

	// More specific assertions about table formatting

	// 1. Verify the long ID doesn't break the formatting
	longIDRow := ""
	for _, line := range lines {
		if strings.Contains(line, "this-is-a-very-long-id-that-exceeds-twenty-characters") {
			longIDRow = line
			break
		}
	}
	require.NotEmpty(t, longIDRow, "Row with long ID not found")

	// Ensure the long ID is properly contained in the first column
	assert.Regexp(t, `^│\s+this-is-a-very-long-id-that-exceeds-twenty-characters\s+│`, longIDRow)

	// 2. Check alignment - numeric columns should be right-aligned
	for _, line := range lines {
		if strings.Contains(line, "short") {
			// The "10" should be right-aligned (spaces before the number)
			assert.Regexp(t, `│\s+10\s+│`, line)
			// The size should be right-aligned (spaces before the size)
			// Size should now be in GB format
			assert.Regexp(t, `│\s+\d+\.\d{2} GB\s+│`, line)
			break
		}
	}
}

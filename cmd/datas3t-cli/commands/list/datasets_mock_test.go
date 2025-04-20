package list

import (
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

// formatTestBytesAsGB normalizes all sizes to gigabytes for easier comparison in test
func formatTestBytesAsGB(bytes uint64, precision int) string {
	// Convert to GB with fixed precision
	gbValue := float64(bytes) / (1024 * 1024 * 1024)
	return fmt.Sprintf("%.*f GB", precision, gbValue)
}

func TestFormattedMockOutput(t *testing.T) {
	// This test demonstrates the fixed precision number formatting in a mock table
	if testing.Short() {
		t.Skip("Skipping visual demo test in short mode")
	}

	// Just create the logger for completeness but we don't need it
	_ = slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Mock datasets with various sizes
	mockDatasets := []client.Dataset{
		{ID: "sample-dataset", DatarangeCount: 42, TotalSizeBytes: 1024 * 1024 * 500},
		{ID: "logs-2022", DatarangeCount: 156, TotalSizeBytes: 1024 * 1024 * 1024 * 3.5},
		{ID: "user-activity-data", DatarangeCount: 89, TotalSizeBytes: 1024 * 1024 * 1024 * 1.75},
		{ID: "metrics", DatarangeCount: 12, TotalSizeBytes: 1024 * 1024 * 750.5},
		{ID: "analytics-2023-q1-very-long-id-name", DatarangeCount: 234, TotalSizeBytes: 1024 * 1024 * 1024 * 9.25},
	}

	t.Log("Demonstrating normalized GB formatting with fixed precision (2 decimal places)")

	tw := table.NewWriter()

	// Configure table style
	tw.SetStyle(table.StyleLight)

	// Add header
	tw.AppendHeader(table.Row{"ID", "DATARANGES", "SIZE (GB)"})

	// Add rows with normalized GB sizes and fixed precision
	for _, ds := range mockDatasets {
		tw.AppendRow(table.Row{
			ds.ID,
			ds.DatarangeCount,
			FormatBytesAsGB(uint64(ds.TotalSizeBytes), 2), // Use 2 decimal places
		})
	}

	// Configure column alignment
	tw.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AlignHeader: text.AlignLeft, Align: text.AlignLeft},
		{Number: 2, AlignHeader: text.AlignRight, Align: text.AlignRight},
		{Number: 3, AlignHeader: text.AlignRight, Align: text.AlignRight},
	})

	// Render the table to string and log it
	renderedTable := tw.Render()
	t.Log("\n" + renderedTable)

	t.Log("Note how all sizes are now normalized to gigabytes for direct comparison:")
	t.Log("Before:")
	t.Log("- 500.00 MB (hard to compare with GB values)")
	t.Log("- 3.50 GB")
	t.Log("- 787.00 MB (hard to compare with GB values)")
	t.Log("After:")
	t.Log("- 0.49 GB (directly comparable)")
	t.Log("- 3.50 GB")
	t.Log("- 0.77 GB (directly comparable)")
}

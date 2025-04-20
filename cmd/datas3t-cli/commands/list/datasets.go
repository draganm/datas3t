package list

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/urfave/cli/v2"
)

// formatBytes formats bytes with a fixed number of decimal places for better visual comparison
func formatBytes(bytes uint64, precision int) string {
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

func Command(log *slog.Logger) *cli.Command {
	cfg := struct {
		serverURL string
	}{}

	return &cli.Command{
		Name:  "list",
		Usage: "List all available datasets",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "server-url",
				Required:    true,
				Usage:       "URL of the Datas3t server",
				Destination: &cfg.serverURL,
				EnvVars:     []string{"DATAS3T_SERVER_URL"},
			},
		},
		Action: func(c *cli.Context) error {
			cl, err := client.NewClient(cfg.serverURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			datasets, err := cl.ListDatasets(c.Context)
			if err != nil {
				return fmt.Errorf("failed to list datasets: %w", err)
			}

			if len(datasets) == 0 {
				fmt.Println("No datasets found")
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)

			// Configure table style
			t.SetStyle(table.StyleLight)

			// Add header
			t.AppendHeader(table.Row{"ID", "DATARANGES", "SIZE"})

			// Add rows
			for _, ds := range datasets {
				t.AppendRow(table.Row{
					ds.ID,
					ds.DatarangeCount,
					formatBytes(uint64(ds.TotalSizeBytes), 2), // Use fixed precision (2 decimal places)
				})
			}

			// Configure column alignment
			t.SetColumnConfigs([]table.ColumnConfig{
				{Number: 1, AlignHeader: text.AlignLeft, Align: text.AlignLeft},
				{Number: 2, AlignHeader: text.AlignRight, Align: text.AlignRight},
				{Number: 3, AlignHeader: text.AlignRight, Align: text.AlignRight},
			})

			t.Render()

			return nil
		},
	}
}

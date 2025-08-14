package list

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List all dataranges of a datas3t",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "server-url",
				Value:   "http://localhost:8765",
				Usage:   "Server URL",
				EnvVars: []string{"DATAS3T_SERVER_URL"},
			},
			&cli.StringFlag{
				Name:     "datas3t",
				Usage:    "Datas3t name",
				Required: true,
			},
		},
		Action: listDatarangesAction,
	}
}

func listDatarangesAction(c *cli.Context) error {
	clientInstance := client.NewClient(c.String("server-url"))
	datas3tName := c.String("datas3t")

	// List dataranges
	dataranges, err := clientInstance.ListDataranges(context.Background(), datas3tName)
	if err != nil {
		return fmt.Errorf("failed to list dataranges: %w", err)
	}

	if len(dataranges) == 0 {
		fmt.Printf("No dataranges found for datas3t '%s'\n", datas3tName)
		return nil
	}

	// Create a tabwriter for formatted output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()

	// Print header
	fmt.Fprintln(w, "ID\tRANGE\tSIZE\tOBJECT KEY")
	fmt.Fprintln(w, "---\t-----\t----\t----------")

	// Print each datarange
	for _, dr := range dataranges {
		rangeStr := fmt.Sprintf("%d-%d", dr.MinDatapointKey, dr.MaxDatapointKey)
		sizeStr := formatSize(dr.SizeBytes)
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\n", dr.DatarangeID, rangeStr, sizeStr, dr.DataObjectKey)
	}

	fmt.Printf("\nTotal dataranges: %d\n", len(dataranges))
	return nil
}

// formatSize formats bytes into human-readable format
func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
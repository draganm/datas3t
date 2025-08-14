package delete

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	cfg := struct {
		serverURL      string
		datas3t        string
		firstDatapoint string
		lastDatapoint  string
		force          bool
	}{}

	return &cli.Command{
		Name:  "delete",
		Usage: "Delete dataranges within a specified range",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "server-url",
				Value:       "http://localhost:8765",
				Usage:       "Server URL",
				EnvVars:     []string{"DATAS3T_SERVER_URL"},
				Destination: &cfg.serverURL,
			},
			&cli.StringFlag{
				Name:        "datas3t",
				Usage:       "Datas3t name",
				Required:    true,
				Destination: &cfg.datas3t,
			},
			&cli.StringFlag{
				Name:        "first-datapoint",
				Usage:       "First datapoint of the range to delete",
				Required:    true,
				Destination: &cfg.firstDatapoint,
			},
			&cli.StringFlag{
				Name:        "last-datapoint",
				Usage:       "Last datapoint of the range to delete",
				Required:    true,
				Destination: &cfg.lastDatapoint,
			},
			&cli.BoolFlag{
				Name:        "force",
				Usage:       "Skip confirmation prompt",
				Destination: &cfg.force,
			},
		},
		Action: func(c *cli.Context) error {
			clientInstance := client.NewClient(cfg.serverURL)
			datas3tName := cfg.datas3t

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			// Parse datapoint range
			firstDatapoint, err := strconv.ParseUint(cfg.firstDatapoint, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid first-datapoint '%s': %w", cfg.firstDatapoint, err)
			}

			lastDatapoint, err := strconv.ParseUint(cfg.lastDatapoint, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid last-datapoint '%s': %w", cfg.lastDatapoint, err)
			}

			if firstDatapoint > lastDatapoint {
				return fmt.Errorf("first-datapoint (%d) cannot be greater than last-datapoint (%d)", firstDatapoint, lastDatapoint)
			}

			// List all dataranges for this datas3t
			dataranges, err := clientInstance.ListDataranges(ctx, datas3tName)
			if err != nil {
				return fmt.Errorf("failed to list dataranges: %w", err)
			}

			if len(dataranges) == 0 {
				fmt.Printf("No dataranges found for datas3t '%s'\n", datas3tName)
				return nil
			}

			// Find dataranges that fall within the specified range
			var toDelete []client.DatarangeInfo
			for _, dr := range dataranges {
				// Check if this datarange overlaps with our delete range
				if dr.MinDatapointKey <= int64(lastDatapoint) && dr.MaxDatapointKey >= int64(firstDatapoint) {
					toDelete = append(toDelete, dr)
				}
			}

			if len(toDelete) == 0 {
				fmt.Printf("No dataranges found in range %d-%d\n", firstDatapoint, lastDatapoint)
				return nil
			}

			// Validate boundaries
			// First datapoint must match the first datapoint of the first datarange
			if toDelete[0].MinDatapointKey != int64(firstDatapoint) {
				return fmt.Errorf("first-datapoint %d does not match the boundary of datarange %d-%d",
					firstDatapoint, toDelete[0].MinDatapointKey, toDelete[0].MaxDatapointKey)
			}

			// Last datapoint must match the last datapoint of the last datarange
			lastRange := toDelete[len(toDelete)-1]
			if lastRange.MaxDatapointKey != int64(lastDatapoint) {
				return fmt.Errorf("last-datapoint %d does not match the boundary of datarange %d-%d",
					lastDatapoint, lastRange.MinDatapointKey, lastRange.MaxDatapointKey)
			}

			// Calculate total size
			var totalSize int64
			for _, dr := range toDelete {
				totalSize += dr.SizeBytes
			}

			// Show summary
			fmt.Printf("\n=== Delete Summary ===\n")
			fmt.Printf("Datas3t: %s\n", datas3tName)
			fmt.Printf("Range to delete: %d-%d\n", firstDatapoint, lastDatapoint)
			fmt.Printf("Number of dataranges: %d\n", len(toDelete))
			fmt.Printf("Total size: %s\n\n", formatSize(totalSize))

			fmt.Println("Dataranges to delete:")
			for i, dr := range toDelete {
				fmt.Printf("  %d. ID: %d, Range: %d-%d, Size: %s\n",
					i+1, dr.DatarangeID, dr.MinDatapointKey, dr.MaxDatapointKey, formatSize(dr.SizeBytes))
			}

			// Ask for confirmation unless force flag is set
			if !cfg.force {
				fmt.Printf("\nAre you sure you want to delete these %d dataranges? (y/N): ", len(toDelete))
				
				scanner := bufio.NewScanner(os.Stdin)
				if !scanner.Scan() {
					return fmt.Errorf("failed to read confirmation")
				}
				
				response := strings.TrimSpace(strings.ToLower(scanner.Text()))
				if response != "y" && response != "yes" {
					fmt.Println("Deletion cancelled")
					return nil
				}
			}

			// Delete each datarange
			fmt.Printf("\nDeleting dataranges...\n")
			for i, dr := range toDelete {
				fmt.Printf("Deleting datarange %d/%d (ID: %d, Range: %d-%d)...\n",
					i+1, len(toDelete), dr.DatarangeID, dr.MinDatapointKey, dr.MaxDatapointKey)

				deleteReq := &client.DeleteDatarangeRequest{
					Datas3tName:       datas3tName,
					FirstDatapointKey: uint64(dr.MinDatapointKey),
					LastDatapointKey:  uint64(dr.MaxDatapointKey),
				}

				err = clientInstance.DeleteDatarange(ctx, deleteReq)
				if err != nil {
					return fmt.Errorf("failed to delete datarange %d-%d: %w",
						dr.MinDatapointKey, dr.MaxDatapointKey, err)
				}
			}

			fmt.Printf("\nSuccessfully deleted %d dataranges from datas3t '%s'\n", len(toDelete), datas3tName)
			return nil
		},
	}
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
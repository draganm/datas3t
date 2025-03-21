package commands

import (
	"context"
	"fmt"
	"strconv"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/spf13/cobra"
)

// NewAggregateCommand creates the aggregate command for the CLI
func NewAggregateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "aggregate [dataset-id] [start-key] [end-key]",
		Short: "Aggregate multiple dataranges into a single datarange",
		Long: `Aggregate multiple dataranges within the specified key range into a single datarange.
This operation is atomic and will replace all affected dataranges with a single consolidated datarange.
		
Example:
  datas3t-cli aggregate my-dataset 1 100`,
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Parse arguments
			datasetID := args[0]

			startKey, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid start key: %w", err)
			}

			endKey, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid end key: %w", err)
			}

			if startKey > endKey {
				return fmt.Errorf("start key must be less than or equal to end key")
			}

			// Get server URL from flags
			serverURL, err := cmd.Flags().GetString("server")
			if err != nil {
				return err
			}

			// Create client
			c, err := client.NewClient(serverURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			// Call aggregate endpoint
			result, err := c.AggregateDatarange(context.Background(), datasetID, startKey, endKey)
			if err != nil {
				return fmt.Errorf("failed to aggregate dataranges: %w", err)
			}

			// Display results
			fmt.Printf("Successfully aggregated dataranges:\n")
			fmt.Printf("  Dataset ID:      %s\n", result.DatasetID)
			fmt.Printf("  Start Key:       %d\n", result.StartKey)
			fmt.Printf("  End Key:         %d\n", result.EndKey)
			fmt.Printf("  Ranges Replaced: %d\n", result.RangesReplaced)
			fmt.Printf("  New Object Key:  %s\n", result.NewObjectKey)
			fmt.Printf("  Size (bytes):    %d\n", result.SizeBytes)

			return nil
		},
	}

	return cmd
}

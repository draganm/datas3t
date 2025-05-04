package wait

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/urfave/cli/v2"
)

func Command(log *slog.Logger) *cli.Command {
	cfg := struct {
		serverURL string
	}{}

	return &cli.Command{
		Name:  "wait",
		Usage: "Wait for datasets to reach specific datapoints",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "server-url",
				Required:    true,
				Usage:       "URL of the Datas3t server",
				Destination: &cfg.serverURL,
				EnvVars:     []string{"DATAS3T_SERVER_URL"},
			},
		},
		ArgsUsage: "DATASET1:DATAPOINT1 [DATASET2:DATAPOINT2 ...]",
		Description: `Wait for one or more datasets to reach specific datapoints.
Example:
  datas3t-cli wait --server-url http://localhost:8080 my-dataset:100 another-dataset:500

This will wait until my-dataset has datapoint 100 and another-dataset has datapoint 500.
The command will keep polling until the conditions are met or the operation is cancelled.
Returns 0 if all conditions are met, 1 if any error occurs.`,
		Action: func(c *cli.Context) error {
			// Parse dataset:datapoint arguments
			datasets := make(map[string]uint64)
			for _, arg := range c.Args().Slice() {
				parts := strings.Split(arg, ":")
				if len(parts) != 2 {
					return fmt.Errorf("invalid argument format: %s (expected DATASET:DATAPOINT)", arg)
				}

				datapoint, err := strconv.ParseUint(parts[1], 10, 64)
				if err != nil {
					return fmt.Errorf("invalid datapoint in %s: %w", arg, err)
				}

				datasets[parts[0]] = datapoint
			}

			if len(datasets) == 0 {
				return fmt.Errorf("no datasets specified")
			}

			cl, err := client.NewClient(cfg.serverURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			log.Info("Waiting for datasets to reach specified datapoints",
				"datasets", datasets)

			response, err := cl.WaitDatasets(c.Context, datasets)
			if err != nil {
				statusCode := client.GetStatusCode(err)
				if statusCode == 400 {
					return fmt.Errorf("one or more datasets do not exist: %w", err)
				}
				return fmt.Errorf("failed to wait for datasets: %w", err)
			}

			// Check if all requirements were met
			allMet := true
			for dataset, requiredDP := range datasets {
				currentDP, found := response.Datasets[dataset]
				if !found || currentDP < requiredDP {
					allMet = false
					break
				}
			}

			// Display results in a table
			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.SetStyle(table.StyleLight)
			t.AppendHeader(table.Row{"DATASET", "REQUESTED", "CURRENT", "STATUS"})

			for dataset, requiredDP := range datasets {
				currentDP, found := response.Datasets[dataset]
				if !found {
					t.AppendRow(table.Row{dataset, requiredDP, "N/A", "MISSING"})
					continue
				}

				status := "OK"
				if currentDP < requiredDP {
					status = "WAITING"
				}

				t.AppendRow(table.Row{dataset, requiredDP, currentDP, status})
			}

			t.SetColumnConfigs([]table.ColumnConfig{
				{Number: 1, AlignHeader: text.AlignLeft, Align: text.AlignLeft},
				{Number: 2, AlignHeader: text.AlignRight, Align: text.AlignRight},
				{Number: 3, AlignHeader: text.AlignRight, Align: text.AlignRight},
				{Number: 4, AlignHeader: text.AlignCenter, Align: text.AlignCenter},
			})

			t.Render()

			if !allMet {
				return cli.Exit("Operation completed but not all datasets reached the required datapoints.", 1)
			}

			return nil
		},
	}
}

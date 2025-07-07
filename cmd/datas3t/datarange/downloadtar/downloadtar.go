package downloadtar

import (
	"context"
	"fmt"
	"strconv"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "download-tar",
		Usage: "Download a range of datapoints as a TAR file",
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
			&cli.StringFlag{
				Name:     "first-datapoint",
				Usage:    "First datapoint to download",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "last-datapoint",
				Usage:    "Last datapoint to download",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "output",
				Usage:    "Output TAR file path",
				Required: true,
			},
			&cli.IntFlag{
				Name:  "max-parallelism",
				Usage: "Maximum number of concurrent downloads",
				Value: 4,
			},
			&cli.IntFlag{
				Name:  "max-retries",
				Usage: "Maximum number of retry attempts per chunk",
				Value: 3,
			},
			&cli.Int64Flag{
				Name:  "chunk-size",
				Usage: "Size of each download chunk in bytes",
				Value: 5 * 1024 * 1024, // 5MB
			},
		},
		Action: downloadTarAction,
	}
}

func downloadTarAction(c *cli.Context) error {
	clientInstance := client.NewClient(c.String("server-url"))

	datas3tName := c.String("datas3t")
	outputPath := c.String("output")

	// Parse datapoint range
	firstDatapointStr := c.String("first-datapoint")
	lastDatapointStr := c.String("last-datapoint")

	firstDatapoint, err := strconv.ParseUint(firstDatapointStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid first-datapoint '%s': %w", firstDatapointStr, err)
	}

	lastDatapoint, err := strconv.ParseUint(lastDatapointStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid last-datapoint '%s': %w", lastDatapointStr, err)
	}

	if firstDatapoint > lastDatapoint {
		return fmt.Errorf("first-datapoint (%d) cannot be greater than last-datapoint (%d)", firstDatapoint, lastDatapoint)
	}

	fmt.Printf("Downloading datapoints %d-%d from datas3t '%s' to '%s'...\n", firstDatapoint, lastDatapoint, datas3tName, outputPath)

	err = clientInstance.DownloadDatapointsTarWithOptions(context.Background(), datas3tName, firstDatapoint, lastDatapoint, outputPath, nil)
	if err != nil {
		return fmt.Errorf("failed to download datapoints: %w", err)
	}

	fmt.Printf("Successfully downloaded datapoints to '%s'\n", outputPath)
	return nil
}
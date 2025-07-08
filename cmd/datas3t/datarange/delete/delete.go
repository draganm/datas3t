package delete

import (
	"context"
	"fmt"
	"strconv"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "Delete a datarange",
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
				Usage:    "First datapoint of the datarange to delete",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "last-datapoint",
				Usage:    "Last datapoint of the datarange to delete",
				Required: true,
			},
		},
		Action: deleteDatarangeAction,
	}
}

func deleteDatarangeAction(c *cli.Context) error {
	clientInstance := client.NewClient(c.String("server-url"))

	datas3tName := c.String("datas3t")

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

	fmt.Printf("Deleting datarange %d-%d from datas3t '%s'...\n", firstDatapoint, lastDatapoint, datas3tName)

	// Create delete request
	deleteReq := &client.DeleteDatarangeRequest{
		Datas3tName:       datas3tName,
		FirstDatapointKey: firstDatapoint,
		LastDatapointKey:  lastDatapoint,
	}

	err = clientInstance.DeleteDatarange(context.Background(), deleteReq)
	if err != nil {
		return fmt.Errorf("failed to delete datarange: %w", err)
	}

	fmt.Printf("Successfully deleted datarange %d-%d from datas3t '%s'\n", firstDatapoint, lastDatapoint, datas3tName)
	return nil
}
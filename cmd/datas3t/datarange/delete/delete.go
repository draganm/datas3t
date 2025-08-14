package delete

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
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
	}{}

	return &cli.Command{
		Name:  "delete",
		Usage: "Delete a datarange",
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
				Usage:       "First datapoint of the datarange to delete",
				Required:    true,
				Destination: &cfg.firstDatapoint,
			},
			&cli.StringFlag{
				Name:        "last-datapoint",
				Usage:       "Last datapoint of the datarange to delete",
				Required:    true,
				Destination: &cfg.lastDatapoint,
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

			fmt.Printf("Deleting datarange %d-%d from datas3t '%s'...\n", firstDatapoint, lastDatapoint, datas3tName)

			// Create delete request
			deleteReq := &client.DeleteDatarangeRequest{
				Datas3tName:       datas3tName,
				FirstDatapointKey: firstDatapoint,
				LastDatapointKey:  lastDatapoint,
			}

			err = clientInstance.DeleteDatarange(ctx, deleteReq)
			if err != nil {
				return fmt.Errorf("failed to delete datarange: %w", err)
			}

			fmt.Printf("Successfully deleted datarange %d-%d from datas3t '%s'\n", firstDatapoint, lastDatapoint, datas3tName)
			return nil
		},
	}
}

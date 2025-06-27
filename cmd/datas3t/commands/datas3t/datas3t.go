package datas3t

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/draganm/datas3t/client"
	"github.com/draganm/datas3t/server/datas3t"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "datas3t",
		Usage: "Manage datas3ts",
		Subcommands: []*cli.Command{
			{
				Name:  "add",
				Usage: "Add a new datas3t",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "server-url",
						Value:   "http://localhost:8080",
						Usage:   "Server URL",
						EnvVars: []string{"DATAS3T_SERVER_URL"},
					},
					&cli.StringFlag{
						Name:     "name",
						Usage:    "Datas3t name",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "bucket",
						Usage:    "Bucket configuration name",
						Required: true,
					},
				},
				Action: addDatas3tAction,
			},
			{
				Name:  "list",
				Usage: "List all datas3ts",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "server-url",
						Value:   "http://localhost:8080",
						Usage:   "Server URL",
						EnvVars: []string{"DATAS3T_SERVER_URL"},
					},
					&cli.BoolFlag{
						Name:  "json",
						Usage: "Output as JSON",
					},
				},
				Action: listDatas3tsAction,
			},
		},
	}
}

func addDatas3tAction(c *cli.Context) error {
	client := client.NewClient(c.String("server-url"))

	req := &datas3t.AddDatas3tRequest{
		Name:   c.String("name"),
		Bucket: c.String("bucket"),
	}

	err := client.AddDatas3t(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to add datas3t: %w", err)
	}

	fmt.Printf("Successfully added datas3t '%s'\n", req.Name)
	return nil
}

func listDatas3tsAction(c *cli.Context) error {
	client := client.NewClient(c.String("server-url"))

	datas3ts, err := client.ListDatas3ts(context.Background())
	if err != nil {
		return fmt.Errorf("failed to list datas3ts: %w", err)
	}

	if c.Bool("json") {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(datas3ts)
	}

	fmt.Printf("Found %d datas3t(s):\n\n", len(datas3ts))
	for _, d := range datas3ts {
		fmt.Printf("Name: %s\n", d.Datas3tName)
		fmt.Printf("Bucket: %s\n", d.BucketName)
		fmt.Printf("Dataranges: %d\n", d.DatarangeCount)
		fmt.Printf("Total Datapoints: %d\n", d.TotalDatapoints)
		if d.TotalDatapoints > 0 {
			fmt.Printf("Datapoint Range: %d - %d\n", d.LowestDatapoint, d.HighestDatapoint)
		}
		fmt.Printf("Total Size: %d bytes\n", d.TotalBytes)
		fmt.Println()
	}

	return nil
}

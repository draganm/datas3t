package list

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List all datas3ts",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "server-url",
				Value:   "http://localhost:8765",
				Usage:   "Server URL",
				EnvVars: []string{"DATAS3T_SERVER_URL"},
			},
			&cli.BoolFlag{
				Name:  "json",
				Usage: "Output as JSON",
			},
		},
		Action: listDatas3tsAction,
	}
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
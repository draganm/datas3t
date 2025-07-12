package datasetadd

import (
	"context"
	"fmt"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "add",
		Usage: "Add a new datas3t",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "server-url",
				Value:   "http://localhost:8765",
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
	}
}

func addDatas3tAction(c *cli.Context) error {
	clientInstance := client.NewClient(c.String("server-url"))

	req := &client.AddDatas3tRequest{
		Name:   c.String("name"),
		Bucket: c.String("bucket"),
	}

	err := clientInstance.AddDatas3t(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to add datas3t: %w", err)
	}

	fmt.Printf("Successfully added datas3t '%s'\n", req.Name)
	return nil
}
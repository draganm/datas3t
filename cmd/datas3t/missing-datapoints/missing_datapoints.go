package missing_datapoints

import (
	"context"
	"fmt"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "missing-datapoints",
		Usage: "Check if all datapoints are present in a datas3t",
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

	datas3tName := c.Args().First()
	if datas3tName == "" {
		return fmt.Errorf("datas3t name is required")
	}

	bitmap, err := client.GetDatapointsBitmap(context.Background(), datas3tName)
	if err != nil {
		return fmt.Errorf("failed to get datapoints bitmap: %w", err)
	}

	if bitmap.IsEmpty() {
		return fmt.Errorf("no datapoints found")
	}

	numberOfDatapointsExpected := bitmap.Maximum() - bitmap.Minimum() + 1
	numberOfDatapointsActual := bitmap.GetCardinality()

	if numberOfDatapointsActual != numberOfDatapointsExpected {
		return fmt.Errorf("number of datapoints expected: %d, actual: %d", numberOfDatapointsExpected, numberOfDatapointsActual)
	}

	fmt.Printf("All datapoints are present\n")

	return nil
}

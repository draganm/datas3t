package importcmd

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
		Name:  "import",
		Usage: "Import existing datas3ts from S3 bucket",
		Description: `Scan an S3 bucket for existing datas3t datarange files and import them into the database.

This command will:
1. Scan the specified bucket for objects matching the datas3t pattern:
   datas3t/{datas3t_name}/dataranges/{first_datapoint}-{last_datapoint}-{upload_counter}.tar
2. Create any discovered datas3ts in the database if they don't exist
3. Import all discovered dataranges into the database
4. Update upload counters to prevent future conflicts

This is useful for importing data that was uploaded directly to S3 or for disaster recovery scenarios.`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "server-url",
				Value:   "http://localhost:8765",
				Usage:   "Server URL",
				EnvVars: []string{"DATAS3T_SERVER_URL"},
			},
			&cli.StringFlag{
				Name:     "bucket",
				Usage:    "Bucket configuration name to scan for existing datas3ts",
				Required: true,
			},
			&cli.BoolFlag{
				Name:  "json",
				Usage: "Output results as JSON",
			},
		},
		Action: importDatas3tAction,
	}
}

func importDatas3tAction(c *cli.Context) error {
	client := client.NewClient(c.String("server-url"))

	req := &datas3t.ImportDatas3tRequest{
		BucketName: c.String("bucket"),
	}

	response, err := client.ImportDatas3t(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to import datas3ts: %w", err)
	}

	if c.Bool("json") {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(response)
	}

	fmt.Printf("Import completed successfully!\n\n")
	fmt.Printf("Imported %d datas3t(s):\n", response.ImportedCount)
	
	if response.ImportedCount > 0 {
		for _, datas3tName := range response.ImportedDatas3ts {
			fmt.Printf("  - %s\n", datas3tName)
		}
	} else {
		fmt.Printf("No new datas3ts found to import from bucket '%s'\n", req.BucketName)
	}

	fmt.Println()
	fmt.Printf("You can now list datas3ts to see the imported data:\n")
	fmt.Printf("  datas3t datas3t list\n")

	return nil
}
package datasetclear

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "clear",
		Usage: "Clear all dataranges from a datas3t (keeping the datas3t itself)",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "server-url",
				Value:   "http://localhost:8765",
				Usage:   "Server URL",
				EnvVars: []string{"DATAS3T_SERVER_URL"},
			},
			&cli.StringFlag{
				Name:     "name",
				Usage:    "Datas3t name to clear",
				Required: true,
			},
			&cli.BoolFlag{
				Name:  "force",
				Usage: "Skip confirmation prompt",
			},
		},
		Action: clearDatas3tAction,
	}
}

func clearDatas3tAction(c *cli.Context) error {
	datas3tName := c.String("name")
	force := c.Bool("force")

	// Show confirmation prompt unless --force is used
	if !force {
		fmt.Printf("WARNING: This will delete ALL dataranges from datas3t '%s'.\n", datas3tName)
		fmt.Printf("The datas3t record will be kept, but all stored data will be removed.\n")
		fmt.Printf("This operation cannot be undone.\n\n")
		fmt.Printf("Are you sure you want to proceed? (y/N): ")

		reader := bufio.NewReader(os.Stdin)
		input, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("failed to read user input: %w", err)
		}

		input = strings.TrimSpace(strings.ToLower(input))
		if input != "y" && input != "yes" {
			fmt.Println("Operation cancelled.")
			return nil
		}
	}

	clientInstance := client.NewClient(c.String("server-url"))

	req := &client.ClearDatas3tRequest{
		Name: datas3tName,
	}

	response, err := clientInstance.ClearDatas3t(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to clear datas3t: %w", err)
	}

	fmt.Printf("Successfully cleared datas3t '%s'\n", datas3tName)
	fmt.Printf("- Deleted %d dataranges from database\n", response.DatarangesDeleted)
	fmt.Printf("- Scheduled %d S3 objects for deletion\n", response.ObjectsScheduled)
	
	if response.ObjectsScheduled > 0 {
		fmt.Printf("Note: S3 objects will be deleted by the background worker within 24 hours.\n")
	}

	return nil
}
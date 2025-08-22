package datasetdelete

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
		Name:  "delete",
		Usage: "Delete an empty datas3t (must have no dataranges)",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "server-url",
				Value:   "http://localhost:8765",
				Usage:   "Server URL",
				EnvVars: []string{"DATAS3T_SERVER_URL"},
			},
			&cli.StringFlag{
				Name:     "name",
				Usage:    "Datas3t name to delete",
				Required: true,
			},
			&cli.BoolFlag{
				Name:  "force",
				Usage: "Skip confirmation prompt",
			},
		},
		Action: deleteDatas3tAction,
	}
}

func deleteDatas3tAction(c *cli.Context) error {
	datas3tName := c.String("name")
	force := c.Bool("force")

	// Show confirmation prompt unless --force is used
	if !force {
		fmt.Printf("WARNING: This will permanently delete the datas3t '%s'.\n", datas3tName)
		fmt.Printf("Note: The datas3t must be empty (no dataranges). If it contains data, use 'clear' command first.\n")
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

	req := &client.DeleteDatas3tRequest{
		Name: datas3tName,
	}

	_, err := clientInstance.DeleteDatas3t(context.Background(), req)
	if err != nil {
		// Provide helpful error messages
		if strings.Contains(err.Error(), "cannot delete datas3t") && strings.Contains(err.Error(), "contains") {
			fmt.Printf("Error: Cannot delete datas3t '%s' because it contains dataranges.\n", datas3tName)
			fmt.Printf("Hint: Use 'datas3t clear --name %s' to remove all dataranges first, then try deleting again.\n", datas3tName)
			return fmt.Errorf("datas3t is not empty")
		}
		if strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "not found") {
			return fmt.Errorf("datas3t '%s' does not exist", datas3tName)
		}
		return fmt.Errorf("failed to delete datas3t: %w", err)
	}

	fmt.Printf("Successfully deleted datas3t '%s'\n", datas3tName)

	return nil
}
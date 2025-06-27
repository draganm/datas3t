package bucket

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/draganm/datas3t/client"
	"github.com/draganm/datas3t/server/bucket"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "bucket",
		Usage: "Manage S3 bucket configurations",
		Subcommands: []*cli.Command{
			{
				Name:  "add",
				Usage: "Add a new S3 bucket configuration",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "server-url",
						Value:   "http://localhost:8765",
						Usage:   "Server URL",
						EnvVars: []string{"DATAS3T_SERVER_URL"},
					},
					&cli.StringFlag{
						Name:     "name",
						Usage:    "Bucket configuration name",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "endpoint",
						Usage:    "S3 endpoint (include https:// for TLS)",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "bucket",
						Usage:    "S3 bucket name",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "access-key",
						Usage:    "S3 access key",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "secret-key",
						Usage:    "S3 secret key",
						Required: true,
					},
				},
				Action: addBucketAction,
			},
			{
				Name:  "list",
				Usage: "List all S3 bucket configurations",
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
				Action: listBucketsAction,
			},
		},
	}
}

func addBucketAction(c *cli.Context) error {
	client := client.NewClient(c.String("server-url"))

	bucketInfo := &bucket.BucketInfo{
		Name:      c.String("name"),
		Endpoint:  c.String("endpoint"),
		Bucket:    c.String("bucket"),
		AccessKey: c.String("access-key"),
		SecretKey: c.String("secret-key"),
	}

	err := client.AddBucket(context.Background(), bucketInfo)
	if err != nil {
		return fmt.Errorf("failed to add bucket: %w", err)
	}

	fmt.Printf("Successfully added bucket configuration '%s'\n", bucketInfo.Name)
	return nil
}

func listBucketsAction(c *cli.Context) error {
	client := client.NewClient(c.String("server-url"))

	buckets, err := client.ListBuckets(context.Background())
	if err != nil {
		return fmt.Errorf("failed to list buckets: %w", err)
	}

	if c.Bool("json") {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(buckets)
	}

	fmt.Printf("Found %d bucket configuration(s):\n\n", len(buckets))
	for _, b := range buckets {
		fmt.Printf("Name: %s\n", b.Name)
		fmt.Printf("Endpoint: %s\n", b.Endpoint)
		fmt.Printf("Bucket: %s\n", b.Bucket)
		fmt.Printf("Use TLS: %t\n", bucket.IsEndpointTLS(b.Endpoint))
		fmt.Println()
	}

	return nil
}

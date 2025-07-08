package add

import (
	"context"
	"fmt"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
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
	}
}

func addBucketAction(c *cli.Context) error {
	clientInstance := client.NewClient(c.String("server-url"))

	bucketInfo := &client.BucketInfo{
		Name:      c.String("name"),
		Endpoint:  c.String("endpoint"),
		Bucket:    c.String("bucket"),
		AccessKey: c.String("access-key"),
		SecretKey: c.String("secret-key"),
	}

	err := clientInstance.AddBucket(context.Background(), bucketInfo)
	if err != nil {
		return fmt.Errorf("failed to add bucket: %w", err)
	}

	fmt.Printf("Successfully added bucket configuration '%s'\n", bucketInfo.Name)
	return nil
}
package upload

import (
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {

	cfg := struct {
		id     uint64
		dbName string
	}{}

	return &cli.Command{
		Flags: []cli.Flag{
			&cli.Uint64Flag{
				Name:        "id",
				Required:    true,
				EnvVars:     []string{"ID"},
				Destination: &cfg.id,
			},
			&cli.StringFlag{
				Name:        "db-name",
				Required:    true,
				EnvVars:     []string{"DB_NAME"},
				Destination: &cfg.dbName,
			},
		},
		Name: "upload",
		Action: func(c *cli.Context) error {
			ctx := c.Context

			if c.NArg() == 0 {
				return fmt.Errorf("db name must be provided")
			}

			fileName := c.Args().First()

			cl := client.MustClientFromContext(ctx)

			u, err := cl.GetUploadURL(ctx, cfg.dbName, cfg.id)
			if err != nil {
				return fmt.Errorf("could not get upload url: %w", err)
			}

			f, err := os.Open(fileName)
			if err != nil {
				return fmt.Errorf("could not open file: %w", err)
			}

			defer f.Close()

			req, err := http.NewRequestWithContext(ctx, "PUT", u, f)
			if err != nil {
				return fmt.Errorf("could not create upload request: %w", err)
			}

			res, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("could not perform upload: %w", err)
			}

			defer res.Body.Close()

			if res.StatusCode != http.StatusOK {
				d, _ := io.ReadAll(res.Body)
				return fmt.Errorf("unexpected status %s: %s", res.Status, string(d))
			}

			return nil

		},
	}
}

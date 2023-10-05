package mkdb

import (
	"fmt"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {

	return &cli.Command{
		Name: "mkdb",
		Action: func(c *cli.Context) error {

			if c.NArg() == 0 {
				return fmt.Errorf("db name must be provided")
			}

			dbName := c.Args().First()

			ctx := c.Context

			cl := client.MustClientFromContext(ctx)

			return cl.CreateDB(ctx, dbName)

		},
	}
}

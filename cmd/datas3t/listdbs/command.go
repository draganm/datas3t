package listdbs

import (
	"fmt"
	"os"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {

	return &cli.Command{
		Name: "list-dbs",
		Action: func(c *cli.Context) error {
			ctx := c.Context

			cl := client.MustClientFromContext(ctx)
			dbs, err := cl.ListDBs(ctx)
			if err != nil {
				return fmt.Errorf("could not list dbs: %w", err)
			}

			if len(dbs) == 0 {
				fmt.Fprintln(os.Stderr, "no databases")
			}

			for _, db := range dbs {
				fmt.Println(db)
			}

			return nil

		},
	}
}

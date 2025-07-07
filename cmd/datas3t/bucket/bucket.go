package bucket

import (
	"github.com/draganm/datas3t/cmd/datas3t/bucket/add"
	"github.com/draganm/datas3t/cmd/datas3t/bucket/list"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "bucket",
		Usage: "Manage S3 bucket configurations",
		Subcommands: []*cli.Command{
			add.Command(),
			list.Command(),
		},
	}
}
package datas3t

import (
	"github.com/draganm/datas3t/cmd/datas3t/datas3t/add"
	"github.com/draganm/datas3t/cmd/datas3t/datas3t/importcmd"
	"github.com/draganm/datas3t/cmd/datas3t/datas3t/list"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "datas3t",
		Usage: "Manage datas3ts",
		Subcommands: []*cli.Command{
			add.Command(),
			list.Command(),
			importcmd.Command(),
		},
	}
}
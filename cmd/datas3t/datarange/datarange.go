package datarange

import (
	"github.com/draganm/datas3t/cmd/datas3t/datarange/delete"
	"github.com/draganm/datas3t/cmd/datas3t/datarange/downloadtar"
	"github.com/draganm/datas3t/cmd/datas3t/datarange/uploadtar"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "datarange",
		Usage: "Manage datarange operations",
		Subcommands: []*cli.Command{
			uploadtar.Command(),
			downloadtar.Command(),
			delete.Command(),
		},
	}
}
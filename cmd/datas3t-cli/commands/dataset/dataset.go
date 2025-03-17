package dataset

import (
	"log/slog"

	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/dataset/create"
	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/dataset/get"
	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/dataset/list"
	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/dataset/upload"
	"github.com/urfave/cli/v2"
)

func Command(log *slog.Logger) *cli.Command {
	return &cli.Command{
		Name:  "dataset",
		Usage: "Dataset management commands",

		Subcommands: []*cli.Command{
			create.Command(log),
			get.Command(log),
			list.Command(log),
			upload.Command(log),
		},
	}
}

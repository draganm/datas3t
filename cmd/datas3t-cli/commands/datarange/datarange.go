package datarange

import (
	"log/slog"

	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/create"
	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/datarange/download"
	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/datarange/list"
	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/datarange/upload"
	"github.com/urfave/cli/v2"
)

func Command(log *slog.Logger) *cli.Command {
	return &cli.Command{
		Name:  "datarange",
		Usage: "Datarange management commands",
		Subcommands: []*cli.Command{
			download.Command(log),
			upload.Command(log),
			list.Command(log),
			create.Command(log),
		},
	}
}

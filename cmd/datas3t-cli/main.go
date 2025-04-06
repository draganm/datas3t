package main

import (
	"log/slog"
	"os"

	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/aggregate"
	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/create"
	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/datarange"
	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/delete"
	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/get"
	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/list"
	"github.com/urfave/cli/v2"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	app := &cli.App{
		Name:  "datas3t-cli",
		Usage: "Command line interface for Datas3t",
		Commands: []*cli.Command{
			create.Command(log),
			delete.Command(log),
			list.Command(log),
			datarange.Command(log),
			get.Command(log),
			aggregate.Command(log),
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error("terminated", "error", err)
		os.Exit(1)
	}
}

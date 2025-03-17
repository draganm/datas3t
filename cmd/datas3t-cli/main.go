package main

import (
	"log/slog"
	"os"

	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/dataset"
	"github.com/urfave/cli/v2"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	app := &cli.App{
		Name:  "datas3t-cli",
		Usage: "Command line interface for Datas3t",
		Commands: []*cli.Command{
			dataset.Command(log),
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error("terminated", "error", err)
		os.Exit(1)
	}
}

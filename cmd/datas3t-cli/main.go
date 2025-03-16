package main

import (
	"log/slog"
	"os"

	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/create"
	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/get"
	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/list"
	"github.com/draganm/datas3t/cmd/datas3t-cli/commands/upload"
	"github.com/urfave/cli/v2"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := struct {
		serverURL string
	}{}

	app := &cli.App{
		Name:  "datas3t-cli",
		Usage: "Command line interface for Datas3t",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "server-url",
				Required:    true,
				Usage:       "URL of the Datas3t server",
				Destination: &cfg.serverURL,
			},
		},
		Commands: []*cli.Command{
			create.Command(log),
			get.Command(log),
			upload.Command(log),
			list.Command(log),
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error("terminated", "error", err)
		os.Exit(1)
	}
}

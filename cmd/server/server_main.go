package main

import (
	"log/slog"
	"os"
	"os/signal"

	"github.com/draganm/datas3t/pkg/server"
	"github.com/urfave/cli/v2"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	app := &cli.App{
		Name:  "server",
		Usage: "Datas3t server",
		Action: func(c *cli.Context) error {
			log.Info("Starting datas3t")
			ctx, cancel := signal.NotifyContext(c.Context, os.Interrupt)
			defer cancel()

			return server.Run(
				ctx,
				log,
				"file::memory:?cache=shared",
			)
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error("Error starting datas3t", "error", err)
		os.Exit(1)
	}
}

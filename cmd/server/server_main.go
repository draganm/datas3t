package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"

	"github.com/draganm/datas3t/pkg/server"
	"github.com/urfave/cli/v2"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := struct {
		addr  string
		dbURL string
	}{}

	app := &cli.App{
		Name:  "server",
		Usage: "Datas3t server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "db-url",
				Value:       "file::memory:?cache=shared",
				Usage:       "Database URL",
				Destination: &cfg.dbURL,
			},
			&cli.StringFlag{
				Name:        "addr",
				Value:       ":8080",
				Usage:       "Address to listen on",
				Destination: &cfg.addr,
			},
		},
		Action: func(c *cli.Context) error {
			log.Info("Starting datas3t")
			ctx, cancel := signal.NotifyContext(c.Context, os.Interrupt)
			defer cancel()

			s, err := server.CreateServer(
				ctx,
				log,
				"file::memory:?cache=shared",
			)
			if err != nil {
				return err
			}

			hs := &http.Server{
				Addr:    ":8080",
				Handler: s,
			}

			context.AfterFunc(ctx, func() {
				hs.Close()
			})

			return hs.ListenAndServe()
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error("terminated", "error", err)
		os.Exit(1)
	}
}

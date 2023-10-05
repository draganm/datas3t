package main

import (
	"context"
	"fmt"
	"os"

	"github.com/draganm/datas3t/client"
	"github.com/draganm/datas3t/cmd/datas3t/listdbs"
	"github.com/draganm/datas3t/cmd/datas3t/mkdb"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {

	logger, _ := zap.Config{
		Encoding:    "json",
		Level:       zap.NewAtomicLevelAt(zapcore.DebugLevel),
		OutputPaths: []string{"stdout"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:   "message",
			LevelKey:     "level",
			EncodeLevel:  zapcore.CapitalLevelEncoder,
			TimeKey:      "time",
			EncodeTime:   zapcore.ISO8601TimeEncoder,
			CallerKey:    "caller",
			EncodeCaller: zapcore.ShortCallerEncoder,
		},
	}.Build()

	defer logger.Sync()

	log := zapr.NewLogger(logger)

	cfg := struct {
		apiURL        string
		apiToken      string
		adminAPIToken string
	}{}

	app := &cli.App{
		Flags: []cli.Flag{

			&cli.StringFlag{
				Name:        "api-url",
				EnvVars:     []string{"API_URL"},
				Required:    true,
				Destination: &cfg.apiURL,
			},
			&cli.StringFlag{
				Name:        "api-token",
				EnvVars:     []string{"API_TOKEN"},
				Destination: &cfg.apiToken,
			},
			&cli.StringFlag{
				Name:        "admin-api-token",
				EnvVars:     []string{"ADMIN_API_TOKEN"},
				Destination: &cfg.adminAPIToken,
			},
		},

		Before: func(ctx *cli.Context) error {

			cl, err := client.NewClient(cfg.apiURL, client.Options{APIToken: cfg.apiToken, AdminAPIToken: cfg.adminAPIToken})
			if err != nil {
				return fmt.Errorf("could not create client: %w", err)
			}

			ctx.Context = client.ContextWithClient(ctx.Context, cl)

			return nil

		},
		Commands: []*cli.Command{
			listdbs.Command(),
			mkdb.Command(),
		},
	}
	err := app.RunContext(logr.NewContext(context.Background(), log), os.Args)
	if err != nil {
		log.Error(err, "exiting")
	}
}

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/draganm/datas3t/server"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/urfave/cli/v2"
	"github.com/urfave/negroni"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
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

	serverConfig := server.S3Config{}

	httpConfig := struct {
		addr      string
		adminAddr string
	}{}

	authConfig := struct {
		apiToken      string
		adminAPIToken string
	}{}

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "addr",
				Value:       ":5000",
				EnvVars:     []string{"ADDR"},
				Destination: &httpConfig.addr,
			},
			&cli.StringFlag{
				Name:        "admin-addr",
				Value:       ":5001",
				EnvVars:     []string{"ADMIN_ADDR"},
				Destination: &httpConfig.adminAddr,
			},
			&cli.StringFlag{
				Name:        "aws-endpoint-url-s3",
				EnvVars:     []string{"AWS_ENDPOINT_URL_S3"},
				Destination: &serverConfig.S3Endpoint,
			},
			&cli.BoolFlag{
				Usage:       "when aws-endpoint-url-s3 is set, configure if the bucket name will be pre-pended to the hostname",
				Name:        "hostname-immutable",
				EnvVars:     []string{"HOSTNAME_IMMUTABLE"},
				Destination: &serverConfig.HostnameImmutable,
			},
			&cli.StringFlag{
				Name:        "aws-access-key-id",
				EnvVars:     []string{"AWS_ACCESS_KEY_ID"},
				Destination: &serverConfig.AccessKeyID,
			},
			&cli.StringFlag{
				Name:        "aws-secret-access-key",
				EnvVars:     []string{"AWS_SECRET_ACCESS_KEY"},
				Destination: &serverConfig.SecretAccessKey,
			},
			&cli.StringFlag{
				Name:        "bucket-name",
				EnvVars:     []string{"BUCKET_NAME"},
				Required:    true,
				Destination: &serverConfig.BucketName,
			},
			&cli.StringFlag{
				Name:        "prefix",
				EnvVars:     []string{"PREFIX"},
				Required:    true,
				Destination: &serverConfig.Prefix,
			},
			&cli.StringFlag{
				Name:        "api-token",
				EnvVars:     []string{"API_TOKEN"},
				Required:    true,
				Destination: &authConfig.apiToken,
			},
			&cli.StringFlag{
				Name:        "admin-api-token",
				EnvVars:     []string{"ADMIN_API_TOKEN"},
				Required:    true,
				Destination: &authConfig.adminAPIToken,
			},
		},

		Action: func(c *cli.Context) error {

			eg, ctx := errgroup.WithContext(c.Context)
			log := logr.FromContextOrDiscard(ctx)

			server, err := server.OpenServer(ctx, log, serverConfig)
			if err != nil {
				return fmt.Errorf("could not open server: %w", err)
			}

			// serve api
			eg.Go(runHttp(
				ctx,
				log,
				httpConfig.addr,
				"api",
				requireAuth(server.API, authConfig.apiToken),
			))

			// serve admin api
			eg.Go(runHttp(
				ctx,
				log,
				httpConfig.adminAddr,
				"admin-api",
				requireAuth(server.AdminAPI, authConfig.adminAPIToken),
			))

			return eg.Wait()
		},
	}
	err := app.RunContext(logr.NewContext(context.Background(), log), os.Args)
	if err != nil {
		log.Error(err, "exiting")
	}
}

var bearerTokenRegexp = regexp.MustCompile(`^Bearer (.+)$`)

func requireAuth(handler http.Handler, token string) http.Handler {
	if token == "" {
		return handler
	}
	return negroni.New(
		negroni.HandlerFunc(
			func(rw http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
				authHeader := r.Header.Get("autorization")
				groups := bearerTokenRegexp.FindStringSubmatch(authHeader)

				renderNotAllowed := func() {
					rw.Header().Set("WWW-Authenticate", "Bearer")
					rw.WriteHeader(http.StatusUnauthorized)
					rw.Write([]byte(`not allowed`))
				}

				if groups == nil {
					renderNotAllowed()
					return
				}

				if groups[1] != token {
					renderNotAllowed()
					return
				}

				next(rw, r)
			},
		),
	)
}

func runHttp(ctx context.Context, log logr.Logger, addr, name string, handler http.Handler) func() error {

	return func() error {
		log := log.WithValues("name", name)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("could not listen for %s requests: %w", name, err)

		}

		s := &http.Server{
			Handler: handler,
		}

		go func() {
			<-ctx.Done()
			shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			log.Info("graceful shutdown of the server")
			err := s.Shutdown(shutdownContext)
			if errors.Is(err, context.DeadlineExceeded) {
				log.Info("server did not shut down gracefully, forcing close")
				s.Close()
			}
		}()

		log.Info("server started", "addr", l.Addr().String())
		return s.Serve(l)
	}
}

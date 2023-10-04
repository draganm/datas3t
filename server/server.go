package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-chi/chi"
	"github.com/go-logr/logr"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type Server struct {
	Api        http.Handler
	Admin      http.Handler
	client     *s3.Client
	bucketName string
	prefix     string
	log        logr.Logger
	mu         *sync.Mutex
	databases  map[string]bool
}

type S3Config struct {
	S3Endpoint        string
	AccessKeyID       string
	SecretAccessKey   string
	BucketName        string
	Prefix            string
	HostnameImmutable bool
}

func OpenServer(ctx context.Context, log logr.Logger, cf S3Config) (*Server, error) {

	optsFns := [](func(*config.LoadOptions) error){}
	if cf.S3Endpoint != "" {
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               cf.S3Endpoint,
				HostnameImmutable: cf.HostnameImmutable,
			}, nil
		})

		optsFns = append(optsFns, config.WithEndpointResolverWithOptions(resolver))
	}

	cfg, err := config.LoadDefaultConfig(
		ctx,
		optsFns...,
	)

	if cf.AccessKeyID != "" {

		cfg.Credentials = aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     cf.AccessKeyID,
				SecretAccessKey: cf.SecretAccessKey,
				CanExpire:       false,
			}, nil
		})
	}

	if err != nil {
		return nil, fmt.Errorf("could not load default aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	apiRouter := chi.NewRouter()
	adminRouter := chi.NewRouter()

	s := &Server{
		Api:        apiRouter,
		Admin:      adminRouter,
		client:     client,
		bucketName: cf.BucketName,
		prefix:     cf.Prefix,
		log:        log,
		mu:         &sync.Mutex{},
		databases:  map[string]bool{},
	}

	err = s.iterateOverKeys(ctx, cf.Prefix, func(key string) error {
		parts := strings.Split(key, "/")
		if len(parts) != 2 {
			return nil
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("could not iterate over keys: %w", err)
	}

	adminRouter.Put("/api/db/{name}", s.handleCreateDB)

	adminRouter.Get("/api/db", s.handleListDBs)

	return s, nil

}

func (s *Server) iterateOverKeys(ctx context.Context, prefix string, fn func(key string) error) error {
	hasNextPage := true
	var continuationToken *string

	for hasNextPage {

		res, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			MaxKeys:           10_1000,
			Bucket:            aws.String(s.bucketName),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return fmt.Errorf("could not get object list: %w", err)
		}

		for _, o := range res.Contents {
			name := *o.Key
			name = strings.TrimPrefix(name, prefix+"/")
			err = fn(name)
			if err != nil {
				return err
			}
		}

		continuationToken = res.NextContinuationToken
		hasNextPage = continuationToken != nil

	}

	return nil

}

var dbNameRegexp = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-]{0,128}[a-zA-Z0-9]$`)

func (s *Server) dbPrefix(name string) string {
	return path.Join(s.prefix, name)
}

func (s *Server) handleCreateDB(w http.ResponseWriter, r *http.Request) {
	dbName := chi.URLParam(r, "name")
	log := s.log.WithValues("method", r.Method, "path", r.URL.Path, "dbName", dbName)

	if !dbNameRegexp.MatchString(dbName) {
		http.Error(w, "bad request", http.StatusBadRequest)
		log.Error(errors.New("invalid db name"), "bad params")
		return
	}

	dbInfoPath := path.Join(s.dbPrefix(dbName), "datas3t")

	_, err := s.client.PutObject(r.Context(), &s3.PutObjectInput{
		Bucket: &s.bucketName,
		Key:    aws.String(dbInfoPath),
		Body:   bytes.NewReader(nil),
	})

	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		log.Error(err, "bad params")
		return
	}

	s.mu.Lock()
	_, exists := s.databases[dbName]
	if !exists {
		s.databases[dbName] = true
	}
	s.mu.Unlock()

	if exists {
		http.Error(w, "database already exists", http.StatusConflict)
		log.Error(err, "refusing to create existing db")
		return
	}

	w.WriteHeader(http.StatusCreated)

}

func (s *Server) handleListDBs(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	dbs := maps.Keys(s.databases)
	s.mu.Unlock()

	slices.Sort(dbs)

	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(dbs)

}

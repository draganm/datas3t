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
	"github.com/draganm/datas3t/server/db"
	"github.com/draganm/datas3t/server/s3util"
	"github.com/go-chi/chi"
	"github.com/go-logr/logr"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type Server struct {
	API        http.Handler
	client     *s3.Client
	bucketName string
	prefix     string
	log        logr.Logger
	mu         *sync.Mutex
	databases  map[string]*db.DB
	ctx        context.Context
}

type S3Config struct {
	S3Endpoint        string
	AccessKeyID       string
	SecretAccessKey   string
	BucketName        string
	Prefix            string
	HostnameImmutable bool
}

const dbMarker = "datas3t"

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

	s := &Server{
		API:        apiRouter,
		client:     client,
		bucketName: cf.BucketName,
		prefix:     cf.Prefix,
		log:        log,
		mu:         &sync.Mutex{},
		databases:  map[string]*db.DB{},
		ctx:        ctx,
	}

	foundDBs, err := s3util.GetCommonPrefixes(ctx, s.client, s.bucketName, s.prefix)
	if err != nil {
		return nil, fmt.Errorf("could not list dbs: %w", err)
	}

	for i, v := range foundDBs {
		parts := strings.Split(v, "/")
		foundDBs[i] = parts[1]
	}

	for _, name := range foundDBs {
		log.Info("opening db", "name", name)
		db, err := db.OpenDB(ctx, log, client, s.bucketName, s.dbPrefix(name))
		if err != nil {
			return nil, fmt.Errorf("could not open db %s: %w", name, err)
		}

		s.databases[name] = db
	}

	apiRouter.Put("/api/admin/db/{name}", s.handleCreateDB)
	apiRouter.Get("/api/admin/db", s.handleListDBs)
	apiRouter.Post("/api/db/{name}/uploadUrl/{id}", s.handleUploadURL)
	apiRouter.Post("/api/db/{name}/downloadUrl/{id}", s.handleDownloadURL)
	apiRouter.Get("/api/db/{name}/lastId", s.handleLastID)

	return s, nil

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

	s.mu.Lock()
	_, exists := s.databases[dbName]
	s.mu.Unlock()

	if exists {
		http.Error(w, "database already exists", http.StatusConflict)
		log.Error(errors.New("db exists"), "refusing to create existing db")
		return
	}

	prefix := s.dbPrefix(dbName)

	dbInfoPath := path.Join(prefix, dbMarker)

	_, err := s.client.PutObject(r.Context(), &s3.PutObjectInput{
		Bucket: &s.bucketName,
		Key:    aws.String(dbInfoPath),
		Body:   bytes.NewReader(nil),
	})

	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		log.Error(err, "could not put db marker object")
		return
	}

	db, err := db.OpenDB(context.Background(), s.log, s.client, s.bucketName, prefix)

	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		log.Error(err, "could start db")
		return
	}

	s.mu.Lock()
	s.databases[dbName] = db
	s.mu.Unlock()

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

func (s *Server) handleUploadURL(w http.ResponseWriter, r *http.Request) {

	dbName := chi.URLParam(r, "name")
	log := s.log.WithValues("method", r.Method, "path", r.URL.Path, "dbName", dbName)

	if !dbNameRegexp.MatchString(dbName) {
		http.Error(w, "bad request", http.StatusBadRequest)
		log.Error(errors.New("invalid db name"), "bad params")
		return
	}

	s.mu.Lock()
	db, found := s.databases[dbName]
	s.mu.Unlock()

	if !found {
		http.Error(w, "no such db", http.StatusNotFound)
		log.Error(errors.New("db not found"), "not found")
		return

	}

	db.HandleUploadURL(w, r)

}

func (s *Server) handleLastID(w http.ResponseWriter, r *http.Request) {

	dbName := chi.URLParam(r, "name")
	log := s.log.WithValues("method", r.Method, "path", r.URL.Path, "dbName", dbName)

	if !dbNameRegexp.MatchString(dbName) {
		http.Error(w, "bad request", http.StatusBadRequest)
		log.Error(errors.New("invalid db name"), "bad params")
		return
	}

	s.mu.Lock()
	db, found := s.databases[dbName]
	s.mu.Unlock()

	if !found {
		http.Error(w, "no such db", http.StatusNotFound)
		log.Error(errors.New("db not found"), "not found")
		return

	}

	db.HandleLastID(w, r)

}

func (s *Server) handleDownloadURL(w http.ResponseWriter, r *http.Request) {

	dbName := chi.URLParam(r, "name")
	log := s.log.WithValues("method", r.Method, "path", r.URL.Path, "dbName", dbName)

	if !dbNameRegexp.MatchString(dbName) {
		http.Error(w, "bad request", http.StatusBadRequest)
		log.Error(errors.New("invalid db name"), "bad params")
		return
	}

	s.mu.Lock()
	db, found := s.databases[dbName]
	s.mu.Unlock()

	if !found {
		http.Error(w, "no such db", http.StatusNotFound)
		log.Error(errors.New("db not found"), "not found")
		return

	}

	db.HandleDownloadURL(w, r)

}

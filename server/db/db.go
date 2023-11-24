package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t/server/s3util"
	"github.com/go-chi/chi"
	"github.com/go-logr/logr"
)

type DB struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	bucket        string
	prefix        string
	ctx           context.Context
	log           logr.Logger
	mu            *sync.Mutex
}

func OpenDB(ctx context.Context, log logr.Logger, client *s3.Client, bucket, prefix string) (*DB, error) {

	presignClient := s3.NewPresignClient(client)

	return &DB{
		client:        client,
		bucket:        bucket,
		prefix:        prefix,
		ctx:           ctx,
		log:           log,
		presignClient: presignClient,
		mu:            new(sync.Mutex),
	}, nil
}

func (d *DB) getLastID(ctx context.Context) (uint64, error) {

	lastID, err := s3util.FindLastObject(
		ctx,
		d.client,
		d.bucket,
		path.Join(d.prefix, "l-0"),
		func(key uint64) string {
			return fmt.Sprintf("%020d", key)
		},
	)
	if err != nil {
		return 0, fmt.Errorf("could not find last ID: %w", err)
	}
	return lastID, nil
}

func (d *DB) HandleLastID(w http.ResponseWriter, r *http.Request) {
	log := d.log.WithValues("method", r.Method, "path", r.URL.Path)
	lastID, err := d.getLastID(r.Context())
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		log.Error(err, "could not perform quick last ID update")
		return
	}
	w.Header().Set("content-type", "text/plain")
	w.Write([]byte(strconv.FormatUint(lastID, 10)))
}

func (d *DB) keyForID(id uint64) string {
	return path.Join(d.prefix, "l-0", fmt.Sprintf("%020d", id))
}

func (d *DB) HandleUploadURL(w http.ResponseWriter, r *http.Request) {
	idString := chi.URLParam(r, "id")
	log := d.log.WithValues("method", r.Method, "path", r.URL.Path)

	id, err := strconv.ParseUint(idString, 10, 64)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		log.Error(errors.New("could not parse id"), "bad params")
		return
	}

	key := d.keyForID(id)

	log.Info("signing for key", "key", key)

	presigned, err := d.presignClient.PresignPutObject(r.Context(), &s3.PutObjectInput{
		Bucket:      aws.String(d.bucket),
		Key:         aws.String(key),
		ContentType: aws.String("application/octet-stream"),
	}, s3.WithPresignExpires(15*time.Minute))

	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		log.Error(err, "could not presign url")
		return
	}

	w.Header().Set("content-type", "text/plain")

	w.Write([]byte(presigned.URL))

}

type bulkURLsResponse struct {
	ID  uint64 `json:"id,string"`
	URL string `json:"url"`
}

func (d *DB) HandleBulkUploadURLs(w http.ResponseWriter, r *http.Request) {
	log := d.log.WithValues("method", r.Method, "path", r.URL.Path)

	fromIDString := chi.URLParam(r, "fromID")
	fromID, err := strconv.ParseUint(fromIDString, 10, 64)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		log.Error(errors.New("could not parse fromID"), "bad params")
		return
	}

	toIDString := chi.URLParam(r, "toID")
	toID, err := strconv.ParseUint(toIDString, 10, 64)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		log.Error(errors.New("could not parse toID"), "bad params")
		return
	}

	w.Header().Set("content-type", "application/json")

	enc := json.NewEncoder(w)

	for id := fromID; id <= toID; id++ {
		key := d.keyForID(id)

		presigned, err := d.presignClient.PresignPutObject(r.Context(), &s3.PutObjectInput{
			Bucket:      aws.String(d.bucket),
			Key:         aws.String(key),
			ContentType: aws.String("application/octet-stream"),
		}, s3.WithPresignExpires(60*time.Minute))

		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			log.Error(err, "could not presign url")
			return
		}

		enc.Encode(bulkURLsResponse{ID: id, URL: presigned.URL})

	}

}

type downloadURLResponse struct {
	URL string `json:"url"`
}

func (d *DB) HandleDownloadURL(w http.ResponseWriter, r *http.Request) {
	idString := chi.URLParam(r, "id")
	log := d.log.WithValues("method", r.Method, "path", r.URL.Path)

	id, err := strconv.ParseUint(idString, 10, 64)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		log.Error(errors.New("could not parse id"), "bad params")
		return
	}

	key := d.keyForID(id)

	log.Info("signing for key", "key", key)

	presigned, err := d.presignClient.PresignGetObject(r.Context(), &s3.GetObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(15*time.Minute))

	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		log.Error(err, "could not presign url")
		return
	}

	w.Header().Set("content-type", "application/json")

	json.NewEncoder(w).Encode(downloadURLResponse{URL: presigned.URL})

}

func (d *DB) HandleBulkDownloadURLs(w http.ResponseWriter, r *http.Request) {
	log := d.log.WithValues("method", r.Method, "path", r.URL.Path)

	fromIDString := chi.URLParam(r, "fromID")
	fromID, err := strconv.ParseUint(fromIDString, 10, 64)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		log.Error(errors.New("could not parse fromID"), "bad params")
		return
	}

	toIDString := chi.URLParam(r, "toID")
	toID, err := strconv.ParseUint(toIDString, 10, 64)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		log.Error(errors.New("could not parse toID"), "bad params")
		return
	}

	w.Header().Set("content-type", "application/json")

	enc := json.NewEncoder(w)

	for id := fromID; id <= toID; id++ {
		key := d.keyForID(id)

		presigned, err := d.presignClient.PresignGetObject(r.Context(), &s3.GetObjectInput{
			Bucket: aws.String(d.bucket),
			Key:    aws.String(key),
		}, s3.WithPresignExpires(60*time.Minute))

		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			log.Error(err, "could not presign url")
			return
		}

		enc.Encode(bulkURLsResponse{ID: id, URL: presigned.URL})

	}

}

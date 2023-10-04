package db

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"regexp"
	"strconv"
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
}

type LevelFile struct {
	Level   int
	FirstID uint64
}

var levelFileMatcher = regexp.MustCompile(`^l-(\d)/(\d{20})$`)

func OpenDB(ctx context.Context, log logr.Logger, client *s3.Client, bucket, prefix string) (*DB, error) {

	levelFiles := []LevelFile{}

	presignClient := s3.NewPresignClient(client)

	err := s3util.IterateOverKeysWithPrefix(ctx, client, bucket, prefix, func(key string) error {
		match := levelFileMatcher.FindStringSubmatch(key)
		if match != nil {
			level, err := strconv.ParseInt(match[1], 10, 64)
			if err != nil {
				return fmt.Errorf("could not parse level %s: %w", match[1], err)
			}
			firstID, err := strconv.ParseUint(match[2], 10, 64)
			if err != nil {
				return fmt.Errorf("could not parse first ID %s: %w", match[2], err)
			}
			levelFiles = append(levelFiles, LevelFile{
				Level:   int(level),
				FirstID: firstID,
			})
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("could not iterate over db keys: %w", err)
	}

	return &DB{
		client:        client,
		bucket:        bucket,
		prefix:        prefix,
		ctx:           ctx,
		log:           log,
		presignClient: presignClient,
	}, nil
}

func (d *DB) LastIDHandler(w http.ResponseWriter, r *http.Request) {
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
		log.Error(errors.New("invalid db name"), "bad params")
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

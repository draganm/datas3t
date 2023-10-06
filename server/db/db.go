package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t/server/s3util"
	"github.com/go-chi/chi"
	"github.com/go-logr/logr"
	"golang.org/x/exp/slices"
)

type DB struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	bucket        string
	prefix        string
	ctx           context.Context
	log           logr.Logger
	mu            *sync.Mutex
	levelFiles    []LevelFile
}

const levelMultiplier = 100

type LevelFile struct {
	Level   int
	FirstID uint64
}

func (l LevelFile) lastID() uint64 {
	lastID := l.FirstID

	numberOfIDsInFile := 1
	for i := 0; i < l.Level; i++ {
		numberOfIDsInFile *= 100
	}

	return lastID + uint64(numberOfIDsInFile) - 1
}

func sortLevelFiles(levelFiles []LevelFile) {
	slices.SortFunc(levelFiles, func(a, b LevelFile) int {
		aid := a.lastID()
		bid := b.lastID()

		if aid < bid {
			return -1
		}

		if aid > bid {
			return 1
		}

		return a.Level - b.Level

	})
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

	sortLevelFiles(levelFiles)

	return &DB{
		client:        client,
		bucket:        bucket,
		prefix:        prefix,
		ctx:           ctx,
		log:           log,
		presignClient: presignClient,
		mu:            new(sync.Mutex),
		levelFiles:    levelFiles,
	}, nil
}

func (d *DB) quickUpdateLastID(ctx context.Context) error {

	prefix := path.Join(d.prefix, "l-0")

	firstKey := ""

	lastID := d.getLastID()
	if lastID != math.MaxUint64 {
		firstKey = fmt.Sprintf("%020d", lastID)
	}

	newLevelFiles := []LevelFile{}

	err := s3util.IterateOverKeysWithPrefixStartingWith(ctx, d.client, d.bucket, prefix, firstKey, func(key string) error {
		d.log.Info("iterating over", "key", key)
		firstID, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			return fmt.Errorf("could not parse first ID %s: %w", key, err)
		}

		newLevelFiles = append(newLevelFiles, LevelFile{
			Level:   0,
			FirstID: firstID,
		})
		return nil
	})

	if err != nil {
		return fmt.Errorf("could not iterate over keys: %w", err)
	}

	d.mu.Lock()
	d.levelFiles = append(d.levelFiles, newLevelFiles...)
	sortLevelFiles(d.levelFiles)
	d.mu.Unlock()

	return nil
}

func (d *DB) getLastID() uint64 {
	lastID := uint64(math.MaxUint64)
	d.mu.Lock()
	if len(d.levelFiles) > 0 {
		lastID = d.levelFiles[len(d.levelFiles)-1].lastID()
	}
	d.mu.Unlock()
	return lastID
}

func (d *DB) HandleLastID(w http.ResponseWriter, r *http.Request) {
	log := d.log.WithValues("method", r.Method, "path", r.URL.Path)
	err := d.quickUpdateLastID(r.Context())
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		log.Error(err, "could not perform quick last ID update")
		return
	}
	lastID := d.getLastID()
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

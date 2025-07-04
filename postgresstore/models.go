// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.29.0

package postgresstore

import (
	"github.com/jackc/pgx/v5/pgtype"
)

type Datarange struct {
	ID              int64
	Datas3tID       int64
	DataObjectKey   string
	IndexObjectKey  string
	MinDatapointKey int64
	MaxDatapointKey int64
	SizeBytes       int64
	CreatedAt       pgtype.Timestamp
	UpdatedAt       pgtype.Timestamp
}

type DatarangeUpload struct {
	ID                  int64
	Datas3tID           int64
	UploadID            string
	DataObjectKey       string
	IndexObjectKey      string
	FirstDatapointIndex int64
	NumberOfDatapoints  int64
	DataSize            int64
	CreatedAt           pgtype.Timestamp
	UpdatedAt           pgtype.Timestamp
}

type Datas3t struct {
	ID            int64
	Name          string
	S3BucketID    int64
	UploadCounter int64
	CreatedAt     pgtype.Timestamp
	UpdatedAt     pgtype.Timestamp
}

type KeysToDelete struct {
	ID                 int64
	PresignedDeleteUrl string
	CreatedAt          pgtype.Timestamp
	UpdatedAt          pgtype.Timestamp
	DeleteAfter        pgtype.Timestamp
}

type S3Bucket struct {
	ID        int64
	Name      string
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	CreatedAt pgtype.Timestamp
	UpdatedAt pgtype.Timestamp
}

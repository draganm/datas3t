package client

import (
	"fmt"
	"regexp"
	"strings"
)

// Bucket-related types (from server/bucket)

type BucketInfo struct {
	Name      string `json:"name"`
	Endpoint  string `json:"endpoint"`
	Bucket    string `json:"bucket"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

type BucketListInfo struct {
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket"`
}

// Dataranges-related types (from server/dataranges)

type UploadDatarangeRequest struct {
	Datas3tName         string `json:"datas3t_name"`
	DataSize            uint64 `json:"data_size"`
	NumberOfDatapoints  uint64 `json:"number_of_datapoints"`
	FirstDatapointIndex uint64 `json:"first_datapoint_index"`
}

type UploadDatarangeResponse struct {
	DatarangeID         int64  `json:"datarange_id"`
	ObjectKey           string `json:"object_key"`
	FirstDatapointIndex uint64 `json:"first_datapoint_index"`
	UseDirectPut        bool   `json:"use_direct_put"`
	
	// For multipart uploads
	PresignedMultipartUploadPutURLs []string `json:"presigned_multipart_upload_urls,omitempty"`
	
	// For direct PUT uploads
	PresignedDataPutURL string `json:"presigned_data_put_url,omitempty"`
	
	// Common fields
	PresignedIndexPutURL string `json:"presigned_index_put_url"`
}

type CompleteUploadRequest struct {
	DatarangeUploadID int64    `json:"datarange_upload_id"`
	UploadIDs         []string `json:"upload_ids,omitempty"` // For multipart uploads
}

type CancelUploadRequest struct {
	DatarangeUploadID int64 `json:"datarange_upload_id"`
}

type StartAggregateRequest struct {
	Datas3tName         string `json:"datas3t_name"`
	FirstDatapointIndex uint64 `json:"first_datapoint_index"`
	LastDatapointIndex  uint64 `json:"last_datapoint_index"`
}

type StartAggregateResponse struct {
	AggregateUploadID int64  `json:"aggregate_upload_id"`
	ObjectKey         string `json:"object_key"`
	
	// Source datarange information
	SourceDatarangeDownloadURLs []DatarangeDownloadURL `json:"source_datarange_download_urls"`
	
	// Upload configuration
	UseDirectPut                    bool     `json:"use_direct_put"`
	PresignedMultipartUploadPutURLs []string `json:"presigned_multipart_upload_urls,omitempty"`
	PresignedDataPutURL             string   `json:"presigned_data_put_url,omitempty"`
	PresignedIndexPutURL            string   `json:"presigned_index_put_url"`
}

type DatarangeDownloadURL struct {
	DatarangeID       int64  `json:"datarange_id"`
	DataObjectKey     string `json:"data_object_key"`
	IndexObjectKey    string `json:"index_object_key"`
	MinDatapointKey   int64  `json:"min_datapoint_key"`
	MaxDatapointKey   int64  `json:"max_datapoint_key"`
	SizeBytes         int64  `json:"size_bytes"`
	PresignedDataURL  string `json:"presigned_data_url"`
	PresignedIndexURL string `json:"presigned_index_url"`
}

type CompleteAggregateRequest struct {
	AggregateUploadID int64    `json:"aggregate_upload_id"`
	UploadIDs         []string `json:"upload_ids,omitempty"` // For multipart uploads
}

type CancelAggregateRequest struct {
	AggregateUploadID int64 `json:"aggregate_upload_id"`
}

type DeleteDatarangeRequest struct {
	Datas3tName       string `json:"datas3t_name"`
	FirstDatapointKey uint64 `json:"first_datapoint_key"`
	LastDatapointKey  uint64 `json:"last_datapoint_key"`
}

// Datas3t-related types (from server/datas3t)

type AddDatas3tRequest struct {
	Name   string `json:"name"`
	Bucket string `json:"bucket"`
}

type ImportDatas3tRequest struct {
	BucketName string `json:"bucket_name"`
}

type ImportDatas3tResponse struct {
	ImportedDatas3ts []string `json:"imported_datas3ts"`
	ImportedCount    int      `json:"imported_count"`
}

type Datas3tInfo struct {
	Datas3tName      string `json:"datas3t_name"`
	BucketName       string `json:"bucket_name"`
	DatarangeCount   int64  `json:"datarange_count"`
	TotalDatapoints  int64  `json:"total_datapoints"`
	LowestDatapoint  int64  `json:"lowest_datapoint"`
	HighestDatapoint int64  `json:"highest_datapoint"`
	TotalBytes       int64  `json:"total_bytes"`
}

// Download-related types (from server/download)

type PreSignDownloadForDatapointsRequest struct {
	Datas3tName    string `json:"datas3t_name"`
	FirstDatapoint uint64 `json:"first_datapoint"`
	LastDatapoint  uint64 `json:"last_datapoint"`
}

type DownloadSegment struct {
	PresignedURL string `json:"presigned_url"`
	Range        string `json:"range"`
}

type PreSignDownloadForDatapointsResponse struct {
	DownloadSegments []DownloadSegment `json:"download_segments"`
}

// Error types

type ValidationError error

// Validation functions and constants

var bucketNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
var datas3tNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// IsEndpointTLS determines if an endpoint uses TLS based on its protocol
func IsEndpointTLS(endpoint string) bool {
	return strings.HasPrefix(endpoint, "https://")
}

// Validate validates the BucketInfo struct
func (r *BucketInfo) Validate() error {
	if !bucketNameRegex.MatchString(r.Name) {
		return ValidationError(fmt.Errorf("invalid bucket name: %s", r.Name))
	}

	if r.Endpoint == "" {
		return ValidationError(fmt.Errorf("endpoint is required"))
	}

	if r.Bucket == "" {
		return ValidationError(fmt.Errorf("bucket is required"))
	}

	return nil
}

// Validate validates the AddDatas3tRequest struct
func (r *AddDatas3tRequest) Validate() error {
	if !datas3tNameRegex.MatchString(r.Name) {
		return ValidationError(fmt.Errorf("invalid datas3t name: %s", r.Name))
	}

	if r.Bucket == "" {
		return ValidationError(fmt.Errorf("bucket is required"))
	}

	return nil
}

// Validate validates the ImportDatas3tRequest struct
func (r *ImportDatas3tRequest) Validate() error {
	if r.BucketName == "" {
		return ValidationError(fmt.Errorf("bucket name is required"))
	}

	return nil
}

// Validate validates the PreSignDownloadForDatapointsRequest struct
func (r *PreSignDownloadForDatapointsRequest) Validate() error {
	if r.Datas3tName == "" {
		return ValidationError(fmt.Errorf("datas3t name is required"))
	}

	if r.FirstDatapoint > r.LastDatapoint {
		return ValidationError(fmt.Errorf("first datapoint (%d) cannot be greater than last datapoint (%d)", r.FirstDatapoint, r.LastDatapoint))
	}

	return nil
}

// Validate validates the UploadDatarangeRequest struct
func (r *UploadDatarangeRequest) Validate() error {
	if r.Datas3tName == "" {
		return ValidationError(fmt.Errorf("datas3t name is required"))
	}

	if r.DataSize == 0 {
		return ValidationError(fmt.Errorf("data size must be greater than 0"))
	}

	if r.NumberOfDatapoints == 0 {
		return ValidationError(fmt.Errorf("number of datapoints must be greater than 0"))
	}

	return nil
}

// Validate validates the StartAggregateRequest struct
func (r *StartAggregateRequest) Validate() error {
	if r.Datas3tName == "" {
		return ValidationError(fmt.Errorf("datas3t name is required"))
	}

	if r.FirstDatapointIndex > r.LastDatapointIndex {
		return ValidationError(fmt.Errorf("first datapoint index (%d) cannot be greater than last datapoint index (%d)", r.FirstDatapointIndex, r.LastDatapointIndex))
	}

	return nil
}

// Validate validates the DeleteDatarangeRequest struct
func (r *DeleteDatarangeRequest) Validate() error {
	if r.Datas3tName == "" {
		return ValidationError(fmt.Errorf("datas3t name is required"))
	}

	if r.FirstDatapointKey > r.LastDatapointKey {
		return ValidationError(fmt.Errorf("first datapoint key (%d) cannot be greater than last datapoint key (%d)", r.FirstDatapointKey, r.LastDatapointKey))
	}

	return nil
}
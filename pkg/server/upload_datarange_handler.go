package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cespare/xxhash/v2"
	"github.com/draganm/datas3t/pkg/server/sqlitestore"
	tarmmap "github.com/draganm/tar-mmap-go"
	"github.com/klauspost/compress/zstd"
)

// HandleUploadDatarange handles POST requests to upload data to a dataset
func (s *Server) HandleUploadDatarange(w http.ResponseWriter, r *http.Request) {

	type UploadDataResponse struct {
		DatasetID     string `json:"dataset_id"`
		NumDataPoints int    `json:"num_data_points"`
	}

	// DatapointMetadata represents the metadata for a single datapoint
	type DatapointMetadata struct {
		ID          uint64 `json:"id,string"`
		BeginOffset uint64 `json:"begin_offset,string"`
		EndOffset   uint64 `json:"end_offset,string"`
		DataHash    string `json:"data_hash"`
	}

	id := r.PathValue("id")
	store := sqlitestore.New(s.DB)

	// Check if dataset exists
	exists, err := store.DatasetExists(r.Context(), id)
	if err != nil {
		s.logger.Error("failed to check if dataset exists", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !exists {
		s.logger.Error("dataset not found", "id", id)
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}

	// Create temporary file for upload
	tmpFile, err := os.CreateTemp(s.uploadsPath, fmt.Sprintf("dataset-%s-*.tar", id))
	if err != nil {
		s.logger.Error("failed to create temporary file", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Close and remove the temporary file when done
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	// Copy uploaded data to the temporary file
	_, err = io.Copy(tmpFile, r.Body)
	if err != nil {
		s.logger.Error("failed to copy uploaded data to temporary file", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Ensure all data is written to disk
	err = tmpFile.Sync()
	if err != nil {
		s.logger.Error("failed to sync temporary file", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Rewind to the beginning of the file
	_, err = tmpFile.Seek(0, 0)
	if err != nil {
		s.logger.Error("failed to seek to beginning of temporary file", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	datapointMetadata, err := extractMetadataAndCheckForGaps(tmpFile.Name())
	if err != nil {
		if errors.Is(err, ErrGapDetected) {
			s.logger.Error("gap detected in file sequence", "error", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.logger.Error("failed to extract file numbers", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get min and max datapoint keys
	minDatapointKey := datapointMetadata[0].ID
	maxDatapointKey := datapointMetadata[len(datapointMetadata)-1].ID

	// Check for overlaps with existing datapoints using a single database query
	hasOverlap, err := store.CheckOverlappingDatapointRange(r.Context(), sqlitestore.CheckOverlappingDatapointRangeParams{
		DatasetName: id,
		NewMin:      int64(minDatapointKey),
		NewMax:      int64(maxDatapointKey),
	})
	if err != nil {
		s.logger.Error("failed to check for overlapping datapoints", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if hasOverlap {
		s.logger.Error("datapoint range overlaps with existing datapoints", "min", minDatapointKey, "max", maxDatapointKey)
		http.Error(w, "datapoint range overlaps with existing datapoints", http.StatusBadRequest)
		return
	}

	// Create S3 object key with the pattern dataset/<dataset_name>/datapoints/<from>-<to>.tar
	objectKey := fmt.Sprintf("dataset/%s/datapoints/%020d-%020d.tar", id, minDatapointKey, maxDatapointKey)

	err = s.uploadDatapointsAndMetadata(r.Context(), tmpFile.Name(), objectKey, datapointMetadata)
	if err != nil {
		s.logger.Error("failed to upload datapoints and metadata", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fileInfo, err := tmpFile.Stat()
	if err != nil {
		s.logger.Error("failed to get file info", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Begin a database transaction
	tx, err := s.DB.BeginTx(r.Context(), nil)
	if err != nil {
		s.logger.Error("failed to begin database transaction", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Ensure transaction is rolled back on error
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	// Create a query with the transaction
	txStore := sqlitestore.New(tx).WithTx(tx)

	// Insert data range into database
	dataRangeID, err := txStore.InsertDataRange(r.Context(), sqlitestore.InsertDataRangeParams{
		DatasetName:     id,
		ObjectKey:       objectKey,
		MinDatapointKey: int64(minDatapointKey),
		MaxDatapointKey: int64(maxDatapointKey),
		SizeBytes:       fileInfo.Size(),
	})
	if err != nil {
		s.logger.Error("failed to insert data range into database", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Insert individual datapoints
	for _, md := range datapointMetadata {
		err = txStore.InsertDatapoint(r.Context(), sqlitestore.InsertDatapointParams{
			DatarangeID:  dataRangeID,
			DatapointKey: int64(md.ID),
			BeginOffset:  int64(md.BeginOffset),
			EndOffset:    int64(md.EndOffset),
		})

		if err != nil {
			s.logger.Error("failed to insert datapoint", "datapoint", md.ID, "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		s.logger.Error("failed to commit transaction", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Set transaction to nil to prevent rollback in defer
	tx = nil

	s.logger.Info("successfully uploaded data to S3",
		"dataset", id,
		"objectKey", objectKey,
		"minKey", minDatapointKey,
		"maxKey", maxDatapointKey,
		"sizeBytes", fileInfo.Size(),
		"datapoints", len(datapointMetadata))

	// Prepare and send the response
	response := UploadDataResponse{
		DatasetID:     id,
		NumDataPoints: len(datapointMetadata),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		s.logger.Error("failed to encode response", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

var filePattern = regexp.MustCompile(`^(\d{20})\..+$`)

var ErrGapDetected = errors.New("gap detected in file sequence")

func extractMetadataAndCheckForGaps(filename string) ([]DatapointMetadata, error) {
	// Open the tar file for validation
	tr, err := tarmmap.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open tar file: %w", err)
	}
	defer tr.Close()

	// Track file sequence numbers
	var fileNumbers []uint64

	// Map to store section information for each sequence number
	sectionMap := make(map[uint64]tarmmap.TarSection)

	// Validate file names and collect sequence numbers
	for _, section := range tr.Sections {
		matches := filePattern.FindStringSubmatch(filepath.Base(section.Header.Name))
		if matches == nil {
			return nil, fmt.Errorf("invalid file name pattern: %s", section.Header.Name)
		}

		// Parse the sequence number
		seqNumStr := matches[1]
		seqNum, err := strconv.ParseUint(seqNumStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse sequence number: %w", err)
		}

		fileNumbers = append(fileNumbers, seqNum)
		sectionMap[seqNum] = section
	}

	// Check that we have at least one file
	if len(fileNumbers) == 0 {
		return nil, fmt.Errorf("no valid files in tar archive")
	}

	// Sort the sequence numbers
	sort.Slice(fileNumbers, func(i, j int) bool {
		return fileNumbers[i] < fileNumbers[j]
	})

	// Check for gaps in the sequence
	for i := 0; i < len(fileNumbers)-1; i++ {
		if fileNumbers[i+1] != fileNumbers[i]+1 {
			return nil, fmt.Errorf("%w: expected %d, got %d", ErrGapDetected, fileNumbers[i]+1, fileNumbers[i+1])
		}
	}

	// Generate metadata for each datapoint
	var datapointMetadata []DatapointMetadata

	// Process each datapoint to generate metadata
	for _, seqNum := range fileNumbers {
		section := sectionMap[seqNum]

		// Access memory-mapped data directly from the section
		dataBuf := section.Data

		// Calculate xxhash of the data
		h := xxhash.New()
		h.Write(dataBuf)
		hash := fmt.Sprintf("%x", h.Sum64())

		datapointMetadata = append(datapointMetadata, DatapointMetadata{
			ID:          seqNum,
			BeginOffset: section.HeaderOffset,
			EndOffset:   section.EndOfDataOffset,
			DataHash:    hash,
		})
	}

	return datapointMetadata, nil
}

func (s *Server) uploadDatapointsAndMetadata(ctx context.Context, filename, objectKey string, datapointMetadata []DatapointMetadata) error {
	// Create compressed metadata
	metadataJSON, err := json.Marshal(datapointMetadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata to JSON: %w", err)
	}

	// Compress metadata with zstd best compression
	var compressedBuf bytes.Buffer
	encoder, err := zstd.NewWriter(&compressedBuf, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	_, err = encoder.Write(metadataJSON)
	if err != nil {
		return fmt.Errorf("failed to compress metadata: %w", err)
	}

	// Close the encoder to flush any remaining data
	err = encoder.Close()
	if err != nil {
		return fmt.Errorf("failed to close zstd encoder: %w", err)
	}

	// Upload file to S3
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	_, err = s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file to S3: %w", err)
	}

	// Upload metadata file to S3
	metadataKey := objectKey + ".metadata"

	_, err = s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(metadataKey),
		Body:        bytes.NewReader(compressedBuf.Bytes()),
		ContentType: aws.String("application/zstd"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload metadata file to S3: %w", err)
	}

	return nil

}

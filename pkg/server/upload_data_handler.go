package server

import (
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
	"github.com/draganm/datas3t/pkg/server/sqlitestore"
	tarmmap "github.com/draganm/tar-mmap-go"
)

// HandleUploadData handles POST requests to upload data to a dataset
func (s *Server) HandleUploadData(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	store := sqlitestore.New(s.db)

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

	// Open the tar file for validation
	tr, err := tarmmap.Open(tmpFile.Name())
	if err != nil {
		s.logger.Error("failed to open tar file", "error", err)
		http.Error(w, "invalid tar file", http.StatusBadRequest)
		return
	}
	defer tr.Close()

	// Define pattern for file names
	filePattern := regexp.MustCompile(`^(\d{20})\..+$`)

	// Track file sequence numbers
	var fileNumbers []uint64

	// Validate file names and collect sequence numbers
	for _, section := range tr.Sections {
		matches := filePattern.FindStringSubmatch(filepath.Base(section.Header.Name))
		if matches == nil {
			s.logger.Error("invalid file name pattern", "filename", section.Header.Name)
			http.Error(w, fmt.Sprintf("invalid file name pattern: %s", section.Header.Name), http.StatusBadRequest)
			return
		}

		// Parse the sequence number
		seqNumStr := matches[1]
		seqNum, err := strconv.ParseUint(seqNumStr, 10, 64)
		if err != nil {
			s.logger.Error("failed to parse sequence number", "filename", section.Header.Name, "error", err)
			http.Error(w, fmt.Sprintf("invalid sequence number in file name: %s", section.Header.Name), http.StatusBadRequest)
			return
		}

		fileNumbers = append(fileNumbers, seqNum)
	}

	// Check that we have at least one file
	if len(fileNumbers) == 0 {
		s.logger.Error("no valid files in tar archive")
		http.Error(w, "no valid files in tar archive", http.StatusBadRequest)
		return
	}

	// Sort the sequence numbers
	sort.Slice(fileNumbers, func(i, j int) bool {
		return fileNumbers[i] < fileNumbers[j]
	})

	// Check for gaps in the sequence
	for i := 0; i < len(fileNumbers)-1; i++ {
		if fileNumbers[i+1] != fileNumbers[i]+1 {
			s.logger.Error("gap detected in file sequence", "expected", fileNumbers[i]+1, "got", fileNumbers[i+1])
			http.Error(w, fmt.Sprintf("gap in file sequence: expected %d, got %d", fileNumbers[i]+1, fileNumbers[i+1]), http.StatusBadRequest)
			return
		}
	}

	// Get min and max datapoint keys
	minDatapointKey := fileNumbers[0]
	maxDatapointKey := fileNumbers[len(fileNumbers)-1]

	// Create S3 object key with the pattern dataset/<dataset_name>/datapoints/<from>-<to>.tar
	objectKey := fmt.Sprintf("dataset/%s/datapoints/%020d-%020d.tar", id, minDatapointKey, maxDatapointKey)

	// Upload file to S3
	file, err := os.Open(tmpFile.Name())
	if err != nil {
		s.logger.Error("failed to open temporary file for S3 upload", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer file.Close()

	_, err = s.s3Client.PutObject(r.Context(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
		Body:   file,
	})
	if err != nil {
		s.logger.Error("failed to upload file to S3", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Insert data range into database
	err = store.InsertDataRange(r.Context(), sqlitestore.InsertDataRangeParams{
		DatasetName:     id,
		ObjectKey:       objectKey,
		MinDatapointKey: int64(minDatapointKey),
		MaxDatapointKey: int64(maxDatapointKey),
	})
	if err != nil {
		s.logger.Error("failed to insert data range into database", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.logger.Info("successfully uploaded data to S3",
		"dataset", id,
		"objectKey", objectKey,
		"minKey", minDatapointKey,
		"maxKey", maxDatapointKey)
	w.WriteHeader(http.StatusOK)
}

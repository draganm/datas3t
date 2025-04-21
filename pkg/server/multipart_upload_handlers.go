package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/draganm/datas3t/pkg/server/sqlitestore"
	"github.com/google/uuid"
)

// MultipartUpload represents an ongoing multipart upload
type MultipartUpload struct {
	ID            string
	DatasetID     string
	CreatedAt     time.Time
	Parts         map[int]string // map of part number to temp file path
	PartsMutex    sync.RWMutex
	LastUpdatedAt time.Time
}

// InitiateMultipartUploadResponse represents the response for initiating a multipart upload
type InitiateMultipartUploadResponse struct {
	UploadID  string `json:"upload_id"`
	DatasetID string `json:"dataset_id"`
}

// UploadPartResponse represents the response for uploading a part
type UploadPartResponse struct {
	PartID string `json:"part_id"`
}

// CompleteMultipartUploadRequest represents the request for completing a multipart upload
type CompleteMultipartUploadRequest struct {
	PartIDs []string `json:"part_ids"` // IDs of parts in order
}

// CompleteMultipartUploadResponse represents the response for completing a multipart upload
type CompleteMultipartUploadResponse struct {
	DatasetID     string `json:"dataset_id"`
	NumDataPoints int    `json:"num_data_points"`
}

// MultipartUploadStatus represents the response for getting upload status
type MultipartUploadStatus struct {
	UploadID      string `json:"upload_id"`
	DatasetID     string `json:"dataset_id"`
	CreatedAt     string `json:"created_at"`
	LastUpdated   string `json:"last_updated"`
	UploadedParts []int  `json:"uploaded_parts"`
}

// MultipartUploadInfo provides summary information about a multipart upload
type MultipartUploadInfo struct {
	UploadID    string `json:"upload_id"`
	CreatedAt   string `json:"created_at"`
	LastUpdated string `json:"last_updated"`
	PartCount   int    `json:"part_count"`
}

// HandleInitiateMultipartUpload initializes a new multipart upload for a dataset
func (s *Server) HandleInitiateMultipartUpload(w http.ResponseWriter, r *http.Request) {
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

	// Create a new multipart upload ID
	uploadID := uuid.New().String()

	// Create a new multipart upload
	upload := &MultipartUpload{
		ID:            uploadID,
		DatasetID:     id,
		CreatedAt:     time.Now(),
		Parts:         make(map[int]string),
		LastUpdatedAt: time.Now(),
	}

	// Store the multipart upload
	s.multipartUploadsMutex.Lock()
	if s.multipartUploads == nil {
		s.multipartUploads = make(map[string]*MultipartUpload)
	}
	s.multipartUploads[uploadID] = upload
	s.multipartUploadsMutex.Unlock()

	// Create response
	response := InitiateMultipartUploadResponse{
		UploadID:  uploadID,
		DatasetID: id,
	}

	// Return response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		s.logger.Error("failed to encode response", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	s.logger.Info("initiated multipart upload", "dataset", id, "upload_id", uploadID)
}

// HandleUploadPart handles uploading a part of a multipart upload
func (s *Server) HandleUploadPart(w http.ResponseWriter, r *http.Request) {
	datasetID := r.PathValue("id")
	uploadID := r.PathValue("upload_id")
	partNumberStr := r.PathValue("part_number")

	// Parse part number
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil {
		s.logger.Error("invalid part number", "part_number", partNumberStr)
		http.Error(w, "invalid part number", http.StatusBadRequest)
		return
	}

	// Check if the multipart upload exists
	s.multipartUploadsMutex.RLock()
	upload, exists := s.multipartUploads[uploadID]
	s.multipartUploadsMutex.RUnlock()

	if !exists {
		s.logger.Error("multipart upload not found", "upload_id", uploadID)
		http.Error(w, "multipart upload not found", http.StatusNotFound)
		return
	}

	// Verify the dataset ID matches
	if upload.DatasetID != datasetID {
		s.logger.Error("dataset ID mismatch", "expected", upload.DatasetID, "actual", datasetID)
		http.Error(w, "dataset ID mismatch", http.StatusBadRequest)
		return
	}

	// Create temporary file for part
	partFile, err := os.CreateTemp(s.uploadsPath, fmt.Sprintf("dataset-%s-upload-%s-part-%d-*.tar", datasetID, uploadID, partNumber))
	if err != nil {
		s.logger.Error("failed to create temporary file", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Copy uploaded data to the temporary file
	_, err = io.Copy(partFile, r.Body)
	if err != nil {
		partFile.Close()
		os.Remove(partFile.Name())
		s.logger.Error("failed to copy uploaded data to temporary file", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Ensure all data is written to disk
	err = partFile.Sync()
	if err != nil {
		partFile.Close()
		os.Remove(partFile.Name())
		s.logger.Error("failed to sync temporary file", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	partFile.Close()

	// Store the part file path
	upload.PartsMutex.Lock()
	upload.Parts[partNumber] = partFile.Name()
	upload.LastUpdatedAt = time.Now()
	upload.PartsMutex.Unlock()

	// Create part ID (using the part number for simplicity)
	partID := fmt.Sprintf("%d", partNumber)

	// Create response
	response := UploadPartResponse{
		PartID: partID,
	}

	// Return response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		s.logger.Error("failed to encode response", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	s.logger.Info("uploaded part", "dataset", datasetID, "upload_id", uploadID, "part_number", partNumber)
}

// HandleCompleteMultipartUpload finalizes a multipart upload and processes the combined file
func (s *Server) HandleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request) {
	datasetID := r.PathValue("id")
	uploadID := r.PathValue("upload_id")

	// Check if the multipart upload exists
	s.multipartUploadsMutex.RLock()
	upload, exists := s.multipartUploads[uploadID]
	s.multipartUploadsMutex.RUnlock()

	if !exists {
		s.logger.Error("multipart upload not found", "upload_id", uploadID)
		http.Error(w, "multipart upload not found", http.StatusNotFound)
		return
	}

	// Verify the dataset ID matches
	if upload.DatasetID != datasetID {
		s.logger.Error("dataset ID mismatch", "expected", upload.DatasetID, "actual", datasetID)
		http.Error(w, "dataset ID mismatch", http.StatusBadRequest)
		return
	}

	// Parse the request body to get the part IDs
	var request CompleteMultipartUploadRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		s.logger.Error("failed to decode request body", "error", err)
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Create a temporary file for the combined data
	combinedFile, err := os.CreateTemp(s.uploadsPath, fmt.Sprintf("dataset-%s-combined-*.tar", datasetID))
	if err != nil {
		s.logger.Error("failed to create temporary file", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Close and remove the temporary file when done
	defer func() {
		combinedFile.Close()
		os.Remove(combinedFile.Name())
	}()

	// Create a map of part numbers from part IDs
	partNumbers := make([]int, 0, len(request.PartIDs))
	for _, partIDStr := range request.PartIDs {
		partNum, err := strconv.Atoi(partIDStr)
		if err != nil {
			s.logger.Error("invalid part ID", "part_id", partIDStr)
			http.Error(w, "invalid part ID", http.StatusBadRequest)
			return
		}
		partNumbers = append(partNumbers, partNum)
	}

	// Sort part numbers to ensure correct order
	sort.Ints(partNumbers)

	// Lock the upload parts during processing
	upload.PartsMutex.RLock()
	defer upload.PartsMutex.RUnlock()

	// Check if all parts are available
	for _, partNum := range partNumbers {
		if _, ok := upload.Parts[partNum]; !ok {
			s.logger.Error("missing part", "part_number", partNum)
			http.Error(w, fmt.Sprintf("missing part: %d", partNum), http.StatusBadRequest)
			return
		}
	}

	// Combine the parts in order
	for _, partNum := range partNumbers {
		partFilePath := upload.Parts[partNum]
		partFile, err := os.Open(partFilePath)
		if err != nil {
			s.logger.Error("failed to open part file", "part_file", partFilePath, "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Copy the part data to the combined file
		_, err = io.Copy(combinedFile, partFile)
		partFile.Close()

		if err != nil {
			s.logger.Error("failed to copy part data", "part_file", partFilePath, "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Ensure all data is written to disk
	err = combinedFile.Sync()
	if err != nil {
		s.logger.Error("failed to sync combined file", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Rewind to the beginning of the file
	_, err = combinedFile.Seek(0, 0)
	if err != nil {
		s.logger.Error("failed to seek to beginning of combined file", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Process the combined file similar to HandleUploadDatarange
	datapointMetadata, err := extractMetadataAndCheckForGaps(combinedFile.Name())
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
	store := sqlitestore.New(s.DB)
	hasOverlap, err := store.CheckOverlappingDatapointRange(r.Context(), sqlitestore.CheckOverlappingDatapointRangeParams{
		DatasetName: datasetID,
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
	objectKey := fmt.Sprintf("dataset/%s/datapoints/%020d-%020d.tar", datasetID, minDatapointKey, maxDatapointKey)

	err = s.uploadDatapointsAndMetadata(r.Context(), combinedFile.Name(), objectKey, datapointMetadata)
	if err != nil {
		s.logger.Error("failed to upload datapoints and metadata", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fileInfo, err := combinedFile.Stat()
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
		DatasetName:     datasetID,
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

	// Clean up part files
	for _, partFilePath := range upload.Parts {
		os.Remove(partFilePath)
	}

	// Remove the multipart upload from active uploads
	s.multipartUploadsMutex.Lock()
	delete(s.multipartUploads, uploadID)
	s.multipartUploadsMutex.Unlock()

	s.logger.Info("successfully completed multipart upload",
		"dataset", datasetID,
		"upload_id", uploadID,
		"objectKey", objectKey,
		"minKey", minDatapointKey,
		"maxKey", maxDatapointKey,
		"sizeBytes", fileInfo.Size(),
		"datapoints", len(datapointMetadata))

	// Prepare and send the response
	response := CompleteMultipartUploadResponse{
		DatasetID:     datasetID,
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

// HandleCancelMultipartUpload cancels an ongoing multipart upload and cleans up resources
func (s *Server) HandleCancelMultipartUpload(w http.ResponseWriter, r *http.Request) {
	datasetID := r.PathValue("id")
	uploadID := r.PathValue("upload_id")

	// Check if the multipart upload exists
	s.multipartUploadsMutex.RLock()
	upload, exists := s.multipartUploads[uploadID]
	s.multipartUploadsMutex.RUnlock()

	if !exists {
		s.logger.Error("multipart upload not found", "upload_id", uploadID)
		http.Error(w, "multipart upload not found", http.StatusNotFound)
		return
	}

	// Verify the dataset ID matches
	if upload.DatasetID != datasetID {
		s.logger.Error("dataset ID mismatch", "expected", upload.DatasetID, "actual", datasetID)
		http.Error(w, "dataset ID mismatch", http.StatusBadRequest)
		return
	}

	// Clean up part files
	upload.PartsMutex.Lock()
	for _, partFilePath := range upload.Parts {
		err := os.Remove(partFilePath)
		if err != nil && !os.IsNotExist(err) {
			s.logger.Error("failed to remove part file", "path", partFilePath, "error", err)
		}
	}
	upload.PartsMutex.Unlock()

	// Remove the multipart upload from active uploads
	s.multipartUploadsMutex.Lock()
	delete(s.multipartUploads, uploadID)
	s.multipartUploadsMutex.Unlock()

	s.logger.Info("cancelled multipart upload", "dataset", datasetID, "upload_id", uploadID)

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]string{
		"status":    "cancelled",
		"upload_id": uploadID,
	}
	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		s.logger.Error("failed to encode response", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// HandleGetMultipartUploadStatus returns the status of a multipart upload
func (s *Server) HandleGetMultipartUploadStatus(w http.ResponseWriter, r *http.Request) {
	datasetID := r.PathValue("id")
	uploadID := r.PathValue("upload_id")

	// Check if the multipart upload exists
	s.multipartUploadsMutex.RLock()
	upload, exists := s.multipartUploads[uploadID]
	s.multipartUploadsMutex.RUnlock()

	if !exists {
		s.logger.Error("multipart upload not found", "upload_id", uploadID)
		http.Error(w, "multipart upload not found", http.StatusNotFound)
		return
	}

	// Verify the dataset ID matches
	if upload.DatasetID != datasetID {
		s.logger.Error("dataset ID mismatch", "expected", upload.DatasetID, "actual", datasetID)
		http.Error(w, "dataset ID mismatch", http.StatusBadRequest)
		return
	}

	// Get the list of uploaded parts
	upload.PartsMutex.RLock()
	parts := make([]int, 0, len(upload.Parts))
	for partNum := range upload.Parts {
		parts = append(parts, partNum)
	}
	createdAt := upload.CreatedAt
	lastUpdated := upload.LastUpdatedAt
	upload.PartsMutex.RUnlock()

	// Sort the parts
	sort.Ints(parts)

	// Create response
	response := MultipartUploadStatus{
		UploadID:      uploadID,
		DatasetID:     datasetID,
		CreatedAt:     createdAt.Format(time.RFC3339),
		LastUpdated:   lastUpdated.Format(time.RFC3339),
		UploadedParts: parts,
	}

	// Return response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		s.logger.Error("failed to encode response", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	s.logger.Info("retrieved multipart upload status", "dataset", datasetID, "upload_id", uploadID)
}

// HandleListMultipartUploads returns a list of active multipart uploads for a dataset
func (s *Server) HandleListMultipartUploads(w http.ResponseWriter, r *http.Request) {
	datasetID := r.PathValue("id")
	store := sqlitestore.New(s.DB)

	// Check if dataset exists
	exists, err := store.DatasetExists(r.Context(), datasetID)
	if err != nil {
		s.logger.Error("failed to check if dataset exists", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !exists {
		s.logger.Error("dataset not found", "id", datasetID)
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}

	// Get all multipart uploads for this dataset
	var uploads []MultipartUploadInfo
	s.multipartUploadsMutex.RLock()
	for id, upload := range s.multipartUploads {
		if upload.DatasetID == datasetID {
			upload.PartsMutex.RLock()
			uploads = append(uploads, MultipartUploadInfo{
				UploadID:    id,
				CreatedAt:   upload.CreatedAt.Format(time.RFC3339),
				LastUpdated: upload.LastUpdatedAt.Format(time.RFC3339),
				PartCount:   len(upload.Parts),
			})
			upload.PartsMutex.RUnlock()
		}
	}
	s.multipartUploadsMutex.RUnlock()

	// Return response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"dataset_id": datasetID,
		"uploads":    uploads,
	}

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		s.logger.Error("failed to encode response", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	s.logger.Info("listed multipart uploads", "dataset", datasetID, "count", len(uploads))
}

// StartMultipartUploadCleanupJob initiates a background job to clean up stale multipart uploads
func (s *Server) StartMultipartUploadCleanupJob(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				s.logger.Info("stopping multipart upload cleanup job due to context cancellation")
				return
			case <-ticker.C:
				s.cleanupStaleMultipartUploads()
			}
		}
	}()
	s.logger.Info("started periodic multipart upload cleanup job")
}

// cleanupStaleMultipartUploads removes multipart uploads that have been inactive for too long
func (s *Server) cleanupStaleMultipartUploads() {
	const maxAge = 24 * time.Hour // Maximum age of multipart uploads

	now := time.Now()
	var uploadsToCleanup []*MultipartUpload

	// Find stale uploads
	s.multipartUploadsMutex.RLock()
	for _, upload := range s.multipartUploads {
		if now.Sub(upload.LastUpdatedAt) > maxAge {
			uploadsToCleanup = append(uploadsToCleanup, upload)
		}
	}
	s.multipartUploadsMutex.RUnlock()

	if len(uploadsToCleanup) == 0 {
		return
	}

	s.logger.Info("cleaning up stale multipart uploads", "count", len(uploadsToCleanup))

	// Clean up each stale upload
	for _, upload := range uploadsToCleanup {
		// Clean up part files
		upload.PartsMutex.RLock()
		for _, partFilePath := range upload.Parts {
			err := os.Remove(partFilePath)
			if err != nil && !os.IsNotExist(err) {
				s.logger.Error("failed to remove part file", "path", partFilePath, "error", err)
			}
		}
		upload.PartsMutex.RUnlock()

		// Remove from active uploads
		s.multipartUploadsMutex.Lock()
		delete(s.multipartUploads, upload.ID)
		s.multipartUploadsMutex.Unlock()

		s.logger.Info("cleaned up stale multipart upload", "upload_id", upload.ID, "dataset", upload.DatasetID)
	}
}

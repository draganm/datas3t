package server

import (
	"fmt"
	"net/http"
	"os"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/draganm/datas3t/pkg/server/sqlitestore"
)

// DatapointMetadata represents the metadata for a single datapoint
type DatapointMetadata struct {
	ID          uint64 `json:"id,string"`
	BeginOffset uint64 `json:"begin_offset,string"`
	EndOffset   uint64 `json:"end_offset,string"`
	DataHash    string `json:"data_hash"`
}

// AggregateResponse represents the response returned by the aggregate endpoint
type AggregateResponse struct {
	DatasetID      string `json:"dataset_id"`
	StartKey       int64  `json:"start_key"`
	EndKey         int64  `json:"end_key"`
	RangesReplaced int    `json:"ranges_replaced"`
	NewObjectKey   string `json:"new_object_key"`
	SizeBytes      int64  `json:"size_bytes"`
}

// HandleAggregateDatarange handles POST requests to aggregate multiple dataranges into a single one
func (s *Server) HandleAggregateDatarange(w http.ResponseWriter, r *http.Request) {

	log := s.logger.With("handler", "HandleAggregateDatarange")

	// Extract and validate parameters
	datasetID, start, end, err := s.validateParameters(r)
	if err != nil {
		s.handleError(w, err)
		return
	}

	log.Info("validated parameters", "dataset_id", datasetID, "start", start, "end", end)

	// Create a database store
	store := sqlitestore.New(s.DB)

	// Check if dataset exists and get datarange info
	dataranges, err := s.getDatarangeInfo(r.Context(), store, datasetID, start, end)
	if err != nil {
		s.handleError(w, err)
		return
	}

	if len(dataranges) == 0 {
		s.logger.Error("no dataranges found", "dataset_id", datasetID, "start", start, "end", end)
		http.Error(w, "No dataranges found", http.StatusNotFound)
		return
	}

	if dataranges[0].FirstOffset != 0 {
		s.logger.Error("the datapoint range is splitting the first datarange", "dataset_id", datasetID, "start", start, "end", end)
		http.Error(w, "First offset is not 0", http.StatusBadRequest)
		return
	}

	if dataranges[len(dataranges)-1].LastOffset != dataranges[len(dataranges)-1].SizeBytes-1024 {
		s.logger.Error("the datapoint range is splitting the last datarange", "dataset_id", datasetID, "start", start, "end", end)
		http.Error(w, "First offset is not 0", http.StatusBadRequest)
		return
	}

	// Generate response with presigned URLs
	response, err := s.generateResponse(r.Context(), dataranges)
	if err != nil {
		s.handleError(w, err)
		return
	}

	tf, err := os.CreateTemp(s.uploadsPath, "datarange-aggregated-*")
	if err != nil {
		s.handleError(w, err)
		return
	}
	defer os.Remove(tf.Name())

	objectAndRanges := make([]client.ObjectAndRange, len(response))

	// TODO: check if we can use the same object key for the aggregated datarange

	for i, r := range response {
		objectAndRanges[i] = client.ObjectAndRange{
			GETURL: r.GETURL,
			Start:  r.Start,
			End:    r.End,
		}
	}

	err = client.DownloadDataranges(r.Context(), objectAndRanges, tf)
	if err != nil {
		s.handleError(w, err)
		return
	}

	fileInfo, err := tf.Stat()
	if err != nil {
		s.handleError(w, err)
		return
	}

	err = tf.Sync()
	if err != nil {
		s.handleError(w, err)
		return
	}

	metadata, err := extractMetadataAndCheckForGaps(tf.Name())
	if err != nil {
		s.handleError(w, err)
		return
	}

	minDatapointKey := metadata[0].ID
	maxDatapointKey := metadata[len(metadata)-1].ID

	objectKey := fmt.Sprintf("dataset/%s/datapoints/%020d-%020d.tar", datasetID, minDatapointKey, maxDatapointKey)

	err = s.uploadDatapointsAndMetadata(r.Context(), tf.Name(), objectKey, metadata)
	if err != nil {
		s.handleError(w, err)
		return
	}

	// Update the database by deleting the old dataranges and creating a new one

	// Begin a transaction
	tx, err := s.DB.BeginTx(r.Context(), nil)
	if err != nil {
		s.handleError(w, fmt.Errorf("failed to begin transaction: %w", err))
		return
	}
	defer tx.Rollback() // Rollback if not committed

	txStore := store.WithTx(tx)

	// Delete the old dataranges
	for _, dr := range dataranges {
		err = txStore.DeleteDatarange(r.Context(), dr.FirstOffset)
		if err != nil {
			s.handleError(w, fmt.Errorf("failed to delete old datarange: %w", err))
			return
		}
	}

	// Create the new datarange
	newDatarangeID, err := txStore.InsertDataRange(r.Context(), sqlitestore.InsertDataRangeParams{
		DatasetName:     datasetID,
		ObjectKey:       objectKey,
		MinDatapointKey: int64(minDatapointKey),
		MaxDatapointKey: int64(maxDatapointKey),
		SizeBytes:       fileInfo.Size(),
	})
	if err != nil {
		s.handleError(w, fmt.Errorf("failed to create new datarange: %w", err))
		return
	}

	// Insert datapoints metadata
	for _, dp := range metadata {
		err = txStore.InsertDatapoint(r.Context(), sqlitestore.InsertDatapointParams{
			DatarangeID:  newDatarangeID,
			DatapointKey: int64(dp.ID),
			BeginOffset:  int64(dp.BeginOffset),
			EndOffset:    int64(dp.EndOffset),
		})
		if err != nil {
			s.handleError(w, fmt.Errorf("failed to create datapoint: %w", err))
			return
		}
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		s.handleError(w, fmt.Errorf("failed to commit transaction: %w", err))
		return
	}

	// client.DownloadDataranges(r.Context(), response, tf)

	aggregateResponse := AggregateResponse{
		DatasetID:      datasetID,
		StartKey:       int64(minDatapointKey),
		EndKey:         int64(maxDatapointKey),
		RangesReplaced: len(dataranges),
		NewObjectKey:   objectKey,
		SizeBytes:      fileInfo.Size(),
	}

	s.sendResponse(w, aggregateResponse)
}

package datas3t

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/draganm/datas3t/postgresstore"
)

type ClearDatas3tRequest struct {
	Name string `json:"name"`
}

type ClearDatas3tResponse struct {
	DatarangesDeleted int `json:"dataranges_deleted"`
	ObjectsScheduled  int `json:"objects_scheduled"`
}

func (r *ClearDatas3tRequest) Validate(ctx context.Context) error {
	if r.Name == "" {
		return ValidationError(fmt.Errorf("name is required"))
	}
	if !datas3tNameRegex.MatchString(r.Name) {
		return ValidationError(fmt.Errorf("name must be a valid datas3t name"))
	}
	return nil
}

func (s *Datas3tServer) ClearDatas3t(ctx context.Context, log *slog.Logger, req *ClearDatas3tRequest) (_ *ClearDatas3tResponse, err error) {
	log = log.With("datas3t_name", req.Name)
	log.Info("Starting datas3t clear operation")

	defer func() {
		if err != nil {
			log.Error("Failed to clear datas3t", "error", err)
		} else {
			log.Info("Datas3t clear operation completed successfully")
		}
	}()

	err = req.Validate(ctx)
	if err != nil {
		return nil, err
	}

	// Begin transaction for all database operations
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	txQueries := postgresstore.New(tx)

	// 1. Check if datas3t exists
	datas3tExists, err := s.checkDatas3tExists(ctx, txQueries, req.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to check if datas3t exists: %w", err)
	}
	if !datas3tExists {
		return nil, fmt.Errorf("datas3t '%s' does not exist", req.Name)
	}

	// 2. Get all dataranges for this datas3t
	dataranges, err := txQueries.ClearDatas3tDataranges(ctx, req.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get dataranges for datas3t: %w", err)
	}

	if len(dataranges) == 0 {
		log.Info("No dataranges found for datas3t, nothing to clear")
		return &ClearDatas3tResponse{
			DatarangesDeleted: 0,
			ObjectsScheduled:  0,
		}, nil
	}

	log.Info("Found dataranges to clear", "count", len(dataranges))

	// 3. Schedule all S3 objects for deletion (data + index objects)
	objectsScheduled, err := s.scheduleAllObjectsForDeletion(ctx, log, txQueries, dataranges)
	if err != nil {
		return nil, fmt.Errorf("failed to schedule objects for deletion: %w", err)
	}

	// 4. Delete all dataranges from database
	err = s.deleteDatarangesFromDatabase(ctx, txQueries, dataranges)
	if err != nil {
		return nil, fmt.Errorf("failed to delete dataranges from database: %w", err)
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &ClearDatas3tResponse{
		DatarangesDeleted: len(dataranges),
		ObjectsScheduled:  objectsScheduled,
	}, nil
}

// checkDatas3tExists checks if the datas3t exists
func (s *Datas3tServer) checkDatas3tExists(ctx context.Context, queries *postgresstore.Queries, datas3tName string) (bool, error) {
	existingDatas3ts, err := queries.AllDatas3ts(ctx)
	if err != nil {
		return false, err
	}

	return slices.Contains(existingDatas3ts, datas3tName), nil
}

// scheduleAllObjectsForDeletion schedules all S3 objects for deletion using bucket references
func (s *Datas3tServer) scheduleAllObjectsForDeletion(ctx context.Context, log *slog.Logger, queries *postgresstore.Queries, dataranges []postgresstore.ClearDatas3tDatarangesRow) (int, error) {
	if len(dataranges) == 0 {
		return 0, nil
	}

	// Group objects by bucket for efficient batch operations
	bucketObjects := make(map[int64][]string)
	for _, datarange := range dataranges {
		// Add both data and index objects to the bucket group
		bucketObjects[datarange.S3BucketID] = append(bucketObjects[datarange.S3BucketID], datarange.DataObjectKey, datarange.IndexObjectKey)
	}

	totalObjectsScheduled := 0

	// Schedule objects for deletion in batches per bucket
	for bucketID, objectNames := range bucketObjects {
		err := queries.ScheduleObjectsForDeletion(ctx, postgresstore.ScheduleObjectsForDeletionParams{
			S3BucketID: &bucketID,
			Column2:    objectNames,
		})
		if err != nil {
			log.Error("Failed to schedule objects for deletion", "bucket_id", bucketID, "count", len(objectNames), "error", err)
			continue
		}
		totalObjectsScheduled += len(objectNames)
	}

	log.Info("Scheduled objects for deletion", "total_objects", totalObjectsScheduled)
	return totalObjectsScheduled, nil
}


// deleteDatarangesFromDatabase deletes all dataranges from database using the provided transaction
func (s *Datas3tServer) deleteDatarangesFromDatabase(ctx context.Context, queries *postgresstore.Queries, dataranges []postgresstore.ClearDatas3tDatarangesRow) error {
	if len(dataranges) == 0 {
		return nil
	}

	// Extract datarange IDs
	datarangeIDs := make([]int64, len(dataranges))
	for i, datarange := range dataranges {
		datarangeIDs[i] = datarange.ID
	}

	// Delete all dataranges in a single operation
	err := queries.DeleteDatarangesByIDs(ctx, datarangeIDs)
	if err != nil {
		return fmt.Errorf("failed to delete dataranges: %w", err)
	}

	return nil
}
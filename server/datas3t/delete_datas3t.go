package datas3t

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/draganm/datas3t/postgresstore"
)

type DeleteDatas3tRequest struct {
	Name string `json:"name"`
}

type DeleteDatas3tResponse struct{}

func (r *DeleteDatas3tRequest) Validate(ctx context.Context) error {
	if r.Name == "" {
		return ValidationError(fmt.Errorf("name is required"))
	}
	if !datas3tNameRegex.MatchString(r.Name) {
		return ValidationError(fmt.Errorf("name must be a valid datas3t name"))
	}
	return nil
}

func (s *Datas3tServer) DeleteDatas3t(ctx context.Context, log *slog.Logger, req *DeleteDatas3tRequest) (_ *DeleteDatas3tResponse, err error) {
	log = log.With("datas3t_name", req.Name)
	log.Info("Starting datas3t delete operation")

	defer func() {
		if err != nil {
			log.Error("Failed to delete datas3t", "error", err)
		} else {
			log.Info("Datas3t delete operation completed successfully")
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

	// 2. Check if datas3t has any dataranges
	datarangeCount, err := txQueries.CountDatarangesForDatas3t(ctx, req.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to count dataranges for datas3t: %w", err)
	}

	if datarangeCount > 0 {
		log.Info("Cannot delete datas3t with existing dataranges", "datarange_count", datarangeCount)
		return nil, fmt.Errorf("cannot delete datas3t '%s': it contains %d dataranges. Use 'clear' command first to remove all dataranges", req.Name, datarangeCount)
	}

	// 3. Delete the datas3t record
	err = txQueries.DeleteDatas3t(ctx, req.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to delete datas3t: %w", err)
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Info("Successfully deleted empty datas3t", "name", req.Name)
	return &DeleteDatas3tResponse{}, nil
}
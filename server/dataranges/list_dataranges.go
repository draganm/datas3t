package dataranges

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/draganm/datas3t/postgresstore"
)

type ListDatarangesRequest struct {
	Datas3tName string `json:"datas3t_name"`
}

type ListDatarangesResponse struct {
	Dataranges []DatarangeInfo `json:"dataranges"`
}

type DatarangeInfo struct {
	DatarangeID     int64  `json:"datarange_id"`
	DataObjectKey   string `json:"data_object_key"`
	IndexObjectKey  string `json:"index_object_key"`
	MinDatapointKey int64  `json:"min_datapoint_key"`
	MaxDatapointKey int64  `json:"max_datapoint_key"`
	SizeBytes       int64  `json:"size_bytes"`
}

func (r *ListDatarangesRequest) Validate(ctx context.Context) error {
	if r.Datas3tName == "" {
		return fmt.Errorf("datas3t_name is required")
	}

	return nil
}

func (s *UploadDatarangeServer) ListDataranges(ctx context.Context, log *slog.Logger, req *ListDatarangesRequest) (*ListDatarangesResponse, error) {
	log = log.With("datas3t_name", req.Datas3tName)
	log.Info("Listing dataranges")

	err := req.Validate(ctx)
	if err != nil {
		log.Error("Invalid request", "error", err)
		return nil, err
	}

	queries := postgresstore.New(s.db)
	dbDataranges, err := queries.ListDatarangesForDatas3t(ctx, req.Datas3tName)
	if err != nil {
		log.Error("Failed to list dataranges", "error", err)
		return nil, fmt.Errorf("failed to list dataranges: %w", err)
	}

	// Convert database rows to response format
	dataranges := make([]DatarangeInfo, len(dbDataranges))
	for i, dbDatarange := range dbDataranges {
		dataranges[i] = DatarangeInfo{
			DatarangeID:     dbDatarange.ID,
			DataObjectKey:   dbDatarange.DataObjectKey,
			IndexObjectKey:  dbDatarange.IndexObjectKey,
			MinDatapointKey: dbDatarange.MinDatapointKey,
			MaxDatapointKey: dbDatarange.MaxDatapointKey,
			SizeBytes:       dbDatarange.SizeBytes,
		}
	}

	log.Info("Successfully listed dataranges", "count", len(dataranges))

	return &ListDatarangesResponse{
		Dataranges: dataranges,
	}, nil
}
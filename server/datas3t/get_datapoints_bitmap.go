package datas3t

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/draganm/datas3t/postgresstore"
)

func (s *Datas3tServer) GetDatapointsBitmap(ctx context.Context, log *slog.Logger, datas3tName string) (*roaring64.Bitmap, error) {
	log = log.With("datas3t_name", datas3tName)
	log.Info("Getting datapoints bitmap")

	defer func() {
		log.Info("Datapoints bitmap retrieved")
	}()

	if datas3tName == "" {
		return nil, fmt.Errorf("datas3t_name is required")
	}

	queries := postgresstore.New(s.db)

	// Get all dataranges for the specified datas3t
	dataranges, err := queries.GetDatarangesForDatas3t(ctx, datas3tName)
	if err != nil {
		log.Error("Failed to get dataranges for datas3t", "error", err)
		return nil, fmt.Errorf("failed to get dataranges for datas3t: %w", err)
	}

	// Create a new roaring bitmap
	bitmap := roaring64.New()

	// Add all datapoints from each datarange to the bitmap
	for _, datarange := range dataranges {
		// Add all datapoints from min_datapoint_key to max_datapoint_key (inclusive)
		for datapoint := datarange.MinDatapointKey; datapoint <= datarange.MaxDatapointKey; datapoint++ {
			bitmap.Add(uint64(datapoint))
		}
	}

	log.Info("Datapoints bitmap created",
		"datarange_count", len(dataranges),
		"total_datapoints", bitmap.GetCardinality())

	return bitmap, nil
}

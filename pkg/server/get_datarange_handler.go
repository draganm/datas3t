package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t/pkg/server/sqlitestore"
)

type ObjectAndRange struct {
	GETURL string `json:"get_url"`
	Start  uint64 `json:"start"`
	End    uint64 `json:"end"`
}

type GetDatarangeResponse []ObjectAndRange

// GET /api/v1/datas3t/{id}/datarange/{start}/{end}
func (s *Server) HandleGetDatarange(w http.ResponseWriter, r *http.Request) {
	// Extract and validate parameters
	datasetID, start, end, err := s.validateParameters(r)
	if err != nil {
		s.handleError(w, err)
		return
	}

	// Create a database store
	store := sqlitestore.New(s.DB)

	// Check if dataset exists and get datarange info
	dataranges, err := s.getDatarangeInfo(r.Context(), store, datasetID, start, end)
	if err != nil {
		s.handleError(w, err)
		return
	}

	if len(dataranges) == 0 {
		s.sendEmptyResponse(w)
		return
	}

	// Generate response with presigned URLs
	response, err := s.generateResponse(r.Context(), dataranges)
	if err != nil {
		s.handleError(w, err)
		return
	}

	// Send response
	s.sendResponse(w, response)
}

func (s *Server) validateParameters(r *http.Request) (string, uint64, uint64, error) {
	datasetID := r.PathValue("id")
	startStr := r.PathValue("start")
	endStr := r.PathValue("end")

	start, err := strconv.ParseUint(startStr, 10, 64)
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid start parameter: %w", err)
	}

	end, err := strconv.ParseUint(endStr, 10, 64)
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid end parameter: %w", err)
	}

	if start > end {
		return "", 0, 0, fmt.Errorf("start parameter must be less than or equal to end parameter")
	}

	return datasetID, start, end, nil
}

func (s *Server) getDatarangeInfo(ctx context.Context, store *sqlitestore.Queries, datasetID string, start, end uint64) ([]sqlitestore.GetSectionsOfDatarangesRow, error) {
	exists, err := store.DatasetExists(ctx, datasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to check if dataset exists: %w", err)
	}

	if !exists {
		return nil, fmt.Errorf("dataset not found: %s", datasetID)
	}

	// Get datarange info with offsets in a single query
	dataranges, err := store.GetSectionsOfDataranges(ctx, sqlitestore.GetSectionsOfDatarangesParams{
		DatasetName: datasetID,
		StartKey:    int64(start),
		EndKey:      int64(end),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get datarange info: %w", err)
	}

	return dataranges, nil
}

func (s *Server) generateResponse(ctx context.Context, dataranges []sqlitestore.GetSectionsOfDatarangesRow) (GetDatarangeResponse, error) {
	presignClient := s3.NewPresignClient(s.s3Client)
	var objects GetDatarangeResponse
	var totalBytes uint64 = 0

	for _, dr := range dataranges {
		// Convert interface{} to int64
		firstOffset, ok := dr.FirstOffset.(int64)
		if !ok {
			return GetDatarangeResponse{}, fmt.Errorf("invalid first offset type")
		}
		lastOffset, ok := dr.LastOffset.(int64)
		if !ok {
			return GetDatarangeResponse{}, fmt.Errorf("invalid last offset type")
		}

		// Ensure offsets are aligned to 512 bytes as required
		minOffset := (firstOffset / 512) * 512
		maxOffset := ((lastOffset + 511) / 512) * 512

		req, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(dr.ObjectKey),
		}, func(opts *s3.PresignOptions) {
			opts.Expires = 24 * time.Hour
		})

		if err != nil {
			return GetDatarangeResponse{}, fmt.Errorf("failed to generate presigned URL: %w", err)
		}

		objects = append(objects, ObjectAndRange{
			GETURL: req.URL,
			Start:  uint64(minOffset),
			End:    uint64(maxOffset) - 1,
		})

		totalBytes += uint64(maxOffset - minOffset)
	}

	return objects, nil
}

func (s *Server) sendEmptyResponse(w http.ResponseWriter) {
	response := GetDatarangeResponse{}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Error("failed to encode response", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func (s *Server) sendResponse(w http.ResponseWriter, response GetDatarangeResponse) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Error("failed to encode response", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func (s *Server) handleError(w http.ResponseWriter, err error) {
	s.logger.Error("error occurred", "error", err)
	http.Error(w, fmt.Sprintf("Internal server error: %v", err), http.StatusInternalServerError)
}

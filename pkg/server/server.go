package server

import (
	"context"
	"log/slog"
	"net/http"
)

// Plan for the server
// endpoints
// 	create a datas3t:  PUT /api/v1/datas3t/{id}
// 	get a datas3 info: GET /api/v1/datas3t/{id}
// 	put a datas3t: PATCH /api/v1/datas3t/{id}
// 	post data to a datas3t: POST /api/v1/datas3t/{id}
//  get data for a datas3t range: GET /api/v1/datas3t/{id}/data/{start}/{end}
//  get data for a single data: GET /api/v1/datas3t/{id}/data/{id}

func Run(
	ctx context.Context,
	log *slog.Logger,
) error {
	mux := http.NewServeMux()

	mux.HandleFunc("PUT /api/v1/datas3t/{id}", func(w http.ResponseWriter, r *http.Request) {

	})

	return nil
}

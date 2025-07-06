package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/draganm/datas3t/server/dataranges"
)

func (a *api) completeAggregate(w http.ResponseWriter, r *http.Request) {

	req := &dataranges.CompleteAggregateRequest{}
	err := json.NewDecoder(r.Body).Decode(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = a.s.CompleteAggregate(r.Context(), a.log, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}
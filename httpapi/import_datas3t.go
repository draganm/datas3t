package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/draganm/datas3t/server/datas3t"
)

func (a *api) importDatas3t(w http.ResponseWriter, r *http.Request) {
	var req datas3t.ImportDatas3tRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		var validationErr datas3t.ValidationError
		if errors.As(err, &validationErr) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	response, err := a.s.ImportDatas3t(r.Context(), a.log, &req)
	if err != nil {
		var validationErr datas3t.ValidationError
		if errors.As(err, &validationErr) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
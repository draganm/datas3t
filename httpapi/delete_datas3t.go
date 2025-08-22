package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/draganm/datas3t/server/datas3t"
)

func (a *api) deleteDatas3t(w http.ResponseWriter, r *http.Request) {
	var req datas3t.DeleteDatas3tRequest
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

	response, err := a.s.DeleteDatas3t(r.Context(), a.log, &req)
	if err != nil {
		var validationErr datas3t.ValidationError
		if errors.As(err, &validationErr) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Check if error is about non-empty datas3t
		if strings.Contains(err.Error(), "cannot delete datas3t") && strings.Contains(err.Error(), "contains") {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		// Check if datas3t doesn't exist
		if strings.Contains(err.Error(), "does not exist") {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
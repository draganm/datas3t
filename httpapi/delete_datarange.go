package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/draganm/datas3t/server/dataranges"
)

func (a *api) deleteDatarange(w http.ResponseWriter, r *http.Request) {
	req := &dataranges.DeleteDatarangeRequest{}
	err := json.NewDecoder(r.Body).Decode(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = a.s.DeleteDatarange(r.Context(), a.log, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/draganm/datas3t/server/dataranges"
)

func (a *api) cancelDatarangeUpload(w http.ResponseWriter, r *http.Request) {
	req := &dataranges.CancelUploadRequest{}
	err := json.NewDecoder(r.Body).Decode(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = a.s.CancelDatarangeUpload(r.Context(), a.log, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/draganm/datas3t/server/dataranges"
)

func (a *api) listDataranges(w http.ResponseWriter, r *http.Request) {
	datas3tName := r.URL.Query().Get("datas3t_name")
	if datas3tName == "" {
		http.Error(w, "datas3t_name query parameter is required", http.StatusBadRequest)
		return
	}

	req := &dataranges.ListDatarangesRequest{
		Datas3tName: datas3tName,
	}

	response, err := a.s.ListDataranges(r.Context(), a.log, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
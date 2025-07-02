package httpapi

import (
	"net/http"
)

func (a *api) getDatapointsBitmap(w http.ResponseWriter, r *http.Request) {
	datas3tName := r.URL.Query().Get("datas3t_name")
	if datas3tName == "" {
		http.Error(w, "datas3t_name query parameter is required", http.StatusBadRequest)
		return
	}

	bitmap, err := a.s.GetDatapointsBitmap(r.Context(), a.log, datas3tName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Pack/optimize the bitmap for better compression
	bitmap.RunOptimize()

	// Serialize bitmap to bytes
	bitmapBytes, err := bitmap.ToBytes()
	if err != nil {
		http.Error(w, "failed to serialize bitmap", http.StatusInternalServerError)
		return
	}

	// Return raw bitmap bytes
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(bitmapBytes)
}

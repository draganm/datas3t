package webui

import (
	"log/slog"
	"net/http"

	"github.com/draganm/datas3t/server"
	"github.com/draganm/datas3t/server/dataranges"
)

type Handler struct {
	server *server.Server
	log    *slog.Logger
}

func NewHandler(s *server.Server, log *slog.Logger) *Handler {
	return &Handler{server: s, log: log}
}

func (h *Handler) IndexPage(w http.ResponseWriter, r *http.Request) {
	datas3ts, err := h.server.ListDatas3ts(r.Context(), h.log)
	if err != nil {
		h.log.Error("Failed to list datas3ts", "error", err)
		http.Error(w, "Failed to load datas3ts", http.StatusInternalServerError)
		return
	}

	// Fetch dataranges for each datas3t
	datarangesMap := make(map[string][]dataranges.DatarangeInfo)
	for _, d := range datas3ts {
		req := &dataranges.ListDatarangesRequest{
			Datas3tName: d.Datas3tName,
		}
		resp, err := h.server.ListDataranges(r.Context(), h.log, req)
		if err != nil {
			h.log.Warn("Failed to list dataranges", "error", err, "datas3t", d.Datas3tName)
			// Continue with empty dataranges for this datas3t
			datarangesMap[d.Datas3tName] = []dataranges.DatarangeInfo{}
		} else {
			datarangesMap[d.Datas3tName] = resp.Dataranges
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	component := IndexPage(datas3ts, datarangesMap)
	if err := component.Render(r.Context(), w); err != nil {
		h.log.Error("Failed to render page", "error", err)
	}
}


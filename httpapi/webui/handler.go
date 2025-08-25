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

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	component := IndexPage(datas3ts)
	if err := component.Render(r.Context(), w); err != nil {
		h.log.Error("Failed to render page", "error", err)
	}
}

func (h *Handler) DatarangeChart(w http.ResponseWriter, r *http.Request) {
	datas3tName := r.PathValue("datas3t")

	req := &dataranges.ListDatarangesRequest{
		Datas3tName: datas3tName,
	}

	resp, err := h.server.ListDataranges(r.Context(), h.log, req)
	if err != nil {
		h.log.Error("Failed to list dataranges", "error", err, "datas3t", datas3tName)
		http.Error(w, "Failed to load dataranges", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	component := DatarangeChart(datas3tName, resp.Dataranges)
	if err := component.Render(r.Context(), w); err != nil {
		h.log.Error("Failed to render chart", "error", err)
	}
}
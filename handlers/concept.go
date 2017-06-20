package handlers

import (
	"html/template"
	"net/http"

	"github.com/Financial-Times/ft-metadata-schema-ui/schema"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
)

type ConceptHandler struct {
	schema schema.Schema
}

func NewConceptHandler(schema schema.Schema) *ConceptHandler {
	return &ConceptHandler{schema}
}

func (h *ConceptHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	conceptLabel := vars["concept"]
	c, err := h.schema.Get(conceptLabel)
	if err != nil {
		if err == schema.ErrConceptNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	t, err := template.ParseFiles("templates/concept.html")
	if err != nil {
		log.WithError(err).Error("Error in parsing concept template")
	}
	t.Execute(w, c)
}

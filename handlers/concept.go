package handlers

import (
	"html/template"
	"net/http"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

func ConceptHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	conceptLabel := vars["concept"]
	c := &Concept{conceptLabel}
	t, err := template.ParseFiles("templates/concept.html")
	if err != nil {
		log.WithError(err).Error("Error in parsing concept template")
	}
	t.Execute(w, c)
}

type Concept struct {
	Label string
}

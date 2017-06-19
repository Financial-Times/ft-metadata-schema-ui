package main

import (
	"net/http"

	log "github.com/sirupsen/logrus"

	"github.com/Financial-Times/ft-metadata-schema-ui/handlers"
	"github.com/gorilla/mux"
)

func main() {
	r := mux.NewRouter()
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))
	r.HandleFunc("/{concept}", handlers.ConceptHandler)
	err := http.ListenAndServe(":8080", r)
	if err != nil {
		log.WithError(err).Fatal()
	}
}

package main

import (
	"net/http"

	log "github.com/Sirupsen/logrus"

	"github.com/Financial-Times/ft-metadata-schema-ui/handlers"
	"github.com/Financial-Times/ft-metadata-schema-ui/schema"
	"github.com/gorilla/mux"
)

func main() {
	s, err := schema.New("http://neo4j:pippo@192.168.99.100:7474/db/data")
	if err != nil {
		log.WithError(err).Fatal()
	}

	r := mux.NewRouter()
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))
	r.PathPrefix("/{concept}").Handler(handlers.NewConceptHandler(s))
	err = http.ListenAndServe(":8080", r)
	if err != nil {
		log.WithError(err).Fatal()
	}
}

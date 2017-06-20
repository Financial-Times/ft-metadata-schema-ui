package schema

import (
	"errors"
	"net/http"
	"time"

	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
)

var (
	ErrConceptNotFound = errors.New("concept no found")
)

type Schema interface {
	Get(string) (Concept, error)
}

type schema struct {
	db neoutils.NeoConnection
}

func New(neoEndpoint string) (Schema, error) {
	conf := neoutils.ConnectionConfig{
		BatchSize:     1024,
		Transactional: false,
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 100,
			},
			Timeout: 1 * time.Minute,
		},
		BackgroundConnect: true,
	}
	db, err := neoutils.Connect(neoEndpoint, &conf)
	if err != nil {
		return nil, err
	}

	s := &schema{
		db: db,
	}

	return s, nil
}

func (s *schema) Get(conceptLabel string) (Concept, error) {
	labels, err := s.getConceptLabels()

	if err != nil {
		return Concept{}, err
	}

	if _, found := labels[conceptLabel]; found {
		return Concept{conceptLabel}, nil
	}

	return Concept{}, ErrConceptNotFound
}

func (s *schema) getConceptLabels() (map[string]struct{}, error) {
	nr := NeoResult{}
	r := make(map[string]struct{})

	query := &neoism.CypherQuery{
		Statement: `CALL db.labels()`,
		Result:    &nr,
	}
	err := s.db.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		return r, err
	}
	for _, d := range nr.Data {
		for _, v := range d.Values {
			r[v] = struct{}{}
		}
	}
	return r, nil
}

type NeoResult struct {
	Columns []string                    `json:"columns"`
	Data    []struct{ Values []string } `json:"data"`
}

package schema

import (
	"errors"
	"fmt"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	log "github.com/Sirupsen/logrus"
	"github.com/jmcvetta/neoism"
	"net/http"
	"sync"
	"time"
)

var (
	ErrConceptNotFound = errors.New("concept no found")
)

type Schema interface {
	Get(string) (*Concept, error)
}

type schema struct {
	sync.RWMutex
	db       neoutils.NeoConnection
	concepts map[string]*Concept
}

func New(neoEndpoint string) (Schema, error) {
	conf := neoutils.ConnectionConfig{
		BatchSize:     1024,
		Transactional: false,
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 100,
			},
			Timeout: 5 * time.Minute,
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

	s.init()
	return s, nil
}

func (s *schema) init() {
	s.Lock()
	defer s.Unlock()

	log.Info("Loading Concepts in memory...")
	s.concepts = make(map[string]*Concept)
	log.Info("Getting concepts labels...")
	labels, err := s.getConceptLabels()
	for _, label := range labels {
		c := &Concept{}
		c.Label = label
		fmt.Println(label)
		c.NOfInstances, err = s.getNumberOfInstances(label)
		if err != nil {
			log.WithError(err).WithField("concept", label).Warn("Error in getting number of instances")
		}
		s.concepts[label] = c
	}

	s.populateMoreSpecificTypes()

	log.Info("Concepts loaded")
}

func (s *schema) populateMoreSpecificTypes() {
	log.Info("Inferring sub concepts...")
	labelSets, err := s.getAllDistinctLabelSets()
	if err != nil {
		log.WithError(err).Warn("Impossible to get sub concepts")
		return
	}

	for _, labelSet := range labelSets {
		var c *Concept
		sortedLabelSet, err := mapper.SortTypes(labelSet)
		if err == nil {
			for i, label := range sortedLabelSet {
				if i != 0 {
					if c.MoreSpecificTypes == nil {
						c.MoreSpecificTypes = make(map[string]struct{})
					}
					c.MoreSpecificTypes[label] = struct{}{}
					fmt.Println(c)
				}
				c = s.concepts[label]
			}
		}

	}
}

func (s *schema) getAllDistinctLabelSets() ([][]string, error) {
	nr := []map[string][]string{}
	labelSets := [][]string{}
	query := &neoism.CypherQuery{
		Statement: `MATCH (n) RETURN DISTINCT labels(n) as labelSet`,
		Result:    &nr,
	}
	err := s.db.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		return labelSets, err
	}
	for _, e := range nr {
		labelSets = append(labelSets, e["labelSet"])
	}
	fmt.Println(labelSets)
	return labelSets, nil
}

type labelSetEntity struct {
	labelSet []string `json:"labelSet"`
}

func (s *schema) Get(conceptLabel string) (*Concept, error) {
	s.RLock()
	defer s.RUnlock()

	c, found := s.concepts[conceptLabel]
	if !found {
		return nil, ErrConceptNotFound
	}
	log.Info(c.MoreSpecificTypes)
	return c, nil
}

func (s *schema) getConceptLabels() ([]string, error) {
	nr := []labelEntity{}
	labels := []string{}

	query := &neoism.CypherQuery{
		Statement: `CALL db.labels()`,
		Result:    &nr,
	}
	err := s.db.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		return labels, err
	}
	for _, e := range nr {
		labels = append(labels, e.Label)
	}
	return labels, nil
}

type labelEntity struct {
	Label string `"json:"label"`
}

func (s *schema) getNumberOfInstances(conceptLabel string) (uint64, error) {
	nr := []countEntity{}
	query := &neoism.CypherQuery{
		Statement: `MATCH (n:` + conceptLabel + `) RETURN count(*) as n`,
		Result:    &nr,
	}
	err := s.db.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		return 0, err
	}
	for _, e := range nr {
		return e.N, nil
	}
	return 0, fmt.Errorf("number of instances not found for concept %v", conceptLabel)
}

type countEntity struct {
	N uint64 `json:"n"`
}

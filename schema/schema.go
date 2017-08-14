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

const oneYearInSeconds = 31556926

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
			Timeout: 10 * time.Minute,
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
		c.NOfInstances, err = s.getNumberOfInstances(label)
		if err != nil {
			log.WithError(err).WithField("concept", label).Warn("Error in getting number of instances")
		}
		c.TopInstances, err = s.getTopInstances(label)
		if err != nil {
			log.WithError(err).WithField("concept", label).Warn("Error in getting top instances")
		}
		if len(c.TopInstances) == 0 {
			c.SomeInstances, err = s.getSomeInstances(label)
			if err != nil {
				log.WithError(err).WithField("concept", label).Warn("Error in getting some instances")
			}
		}
		c.Properties, err = s.getProperties(label)
		if err != nil {
			log.WithError(err).WithField("concept", label).Warn("Error in properties")
		}
		s.concepts[label] = c
	}

	s.populateMoreSpecificTypes()

	log.Info("Concepts loaded")
}

func (s *schema) getTopInstances(conceptType string) ([]Instance, error) {
	log.Infof("Getting top instances for %v...", conceptType)
	nr := []Instance{}
	query := &neoism.CypherQuery{
		Statement: `MATCH (n:` + conceptType + `)--(x:Content)
					WHERE x.publishedDateEpoch > {time}
					WITH  n, count(x) AS timesUsed
					ORDER BY timesUsed DESC
					RETURN n.prefLabel, labels(n) as types, timesUsed LIMIT 10`,
		Parameters: neoism.Props{"time": time.Now().Unix() - oneYearInSeconds},
		Result:     &nr,
	}
	err := s.db.CypherBatch([]*neoism.CypherQuery{query})
	return nr, err
}

func (s *schema) getSomeInstances(conceptType string) ([]Instance, error) {
	log.Infof("Getting some instances for %v...", conceptType)
	nr := []Instance{}
	query := &neoism.CypherQuery{
		Statement: `MATCH (n:` + conceptType + `)
					RETURN n.prefLabel, labels(n) as types LIMIT 10`,
		Result: &nr,
	}
	err := s.db.CypherBatch([]*neoism.CypherQuery{query})
	fmt.Println(nr)
	return nr, err
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
		} else {
			for i, label := range labelSet {
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
	return labelSets, nil
}

func (s *schema) getProperties(conceptType string) ([]Property, error) {
	log.Infof("Getting properties for %v...", conceptType)
	nr := []Property{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (z:` + conceptType + `)-[x]->(y) WITH type(x) as t, count(x) as n RETURN t,n`,
		Result:    &nr,
	}
	err := s.db.CypherBatch([]*neoism.CypherQuery{query})
	return nr, err
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

//

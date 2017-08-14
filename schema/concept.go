package schema

import "github.com/Financial-Times/neo-model-utils-go/mapper"

type Concept struct {
	Label             string
	NOfInstances      uint64
	Properties        []Property
	MoreSpecificTypes map[string]struct{}
	TopInstances      []Instance
	SomeInstances     []Instance
}

type Property struct {
	Type         string `json:"t"`
	ExpectedType string
	NOfUsage     uint64 `json:"n"`
}

func (c *Concept) URI() string {
	uris := mapper.TypeURIs([]string{c.Label})
	if len(uris) == 0 {
		return ""
	}
	return uris[0]
}

func (c *Concept) ParentType() string {
	return mapper.ParentType(c.Label)
}

type Instance struct {
	Label     string   `json:"n.prefLabel"`
	TimesUsed uint64   `json:"timesUsed,omitempty"`
	Types     []string `json:"types"`
}

func (i *Instance) MostSpecificType() string {
	t, err := mapper.MostSpecificType(i.Types)
	if err != nil {
		return t
	}
	return i.Types[len(i.Types)-1]
}

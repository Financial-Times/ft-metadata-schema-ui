package schema

type Concept struct {
	Label        string
	NOfInstances uint64
	Properties   []Property
	SubConcepts  map[string]struct{}
}

type Property struct {
	Label        string
	ExpectedType string
	NOfUsage     uint64
}

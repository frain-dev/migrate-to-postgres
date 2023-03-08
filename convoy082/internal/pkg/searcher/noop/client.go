package noopsearcher

import (
	convoy "github.com/frain-dev/migrate-to-postgres/convoy082"
	"github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
)

type NoopSearcher struct{}

func NewNoopSearcher() *NoopSearcher {
	return &NoopSearcher{}
}

func (n *NoopSearcher) Search(collection string, filter *datastore.SearchFilter) ([]string, datastore.PaginationData, error) {
	return make([]string, 0), datastore.PaginationData{}, nil
}

func (n *NoopSearcher) Index(collection string, document convoy.GenericMap) error {
	return nil
}

func (n *NoopSearcher) Remove(collection string, filter *datastore.SearchFilter) error {
	return nil
}

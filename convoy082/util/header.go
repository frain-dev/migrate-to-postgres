package util

import (
	"net/http"
	"strings"

	"github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
)

// ConvertDefaultHeaderToCustomHeader converts http.Header to convoy.HttpHeader
func ConvertDefaultHeaderToCustomHeader(h *http.Header) *datastore.HttpHeader {
	res := make(datastore.HttpHeader)
	for k, v := range *h {
		res[k] = strings.Join(v, " ")
	}

	return &res
}

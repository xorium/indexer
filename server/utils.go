package server

import (
	"bitbucket.org/entrlcom/indexer/core"
	"net/url"
	"strings"
)

const filterPrefix = "filter_"

func parseFilters(filtersQuery string) (core.SearchFilters, error) {
	filtersQuery = strings.TrimLeft(filtersQuery, "?")
	values, err := url.ParseQuery(filtersQuery)
	if err != nil {
		return nil, err
	}

	filters := make(core.SearchFilters)
	for argName, argValues := range values {
		if !strings.HasPrefix(argName, filterPrefix) {
			continue
		}
		argName = argName[len(filterPrefix):]
		filterValues := make([]core.FilterValue, len(argValues))
		for i, argVal := range argValues {
			filterValues[i] = core.FilterValue(argVal)
		}
		filters[core.FilterName(argName)] = filterValues
	}

	return filters, nil
}

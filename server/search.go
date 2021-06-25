package server

import (
	si "bitbucket.org/entrlcom/genproto/gen/go/search/indexer/v1"
	"bitbucket.org/entrlcom/indexer/core"
	"context"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strconv"
)

const (
	defaultPageToken = "1"
	defaultPageSize  = 10
)

func (s *IndexerServer) Search(_ context.Context, r *si.SearchRequest) (*si.SearchResponse, error) {
	searchOpts := core.DefaultSearchOptions
	filters, err := parseFilters(r.GetFilter())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	searchOpts.Filters = filters

	// Срез результатов согласно параметрам пагинации.
	if r.PageToken == "" {
		r.PageToken = defaultPageToken
	}
	if r.PageSize == 0 {
		r.PageSize = defaultPageSize
	}
	pageFrom, err := strconv.Atoi(r.GetPageToken())
	if pageFrom <= 0 {
		pageFrom = 1
	}
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid PageToken")
	}
	pageSize := int(r.GetPageSize())
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}

	from := (pageFrom - 1) * pageSize
	to := from + pageSize
	searchOpts.TopN = to

	result, err := s.manager.DoSearch(r.GetService(), r.GetQ(), searchOpts)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if len(result.Docs) > 0 {
		if to > len(result.Docs) {
			to = len(result.Docs)
		}
		if from >= to {
			return nil, status.Error(
				codes.InvalidArgument,
				fmt.Sprintf("invalid range %d-%d", from, to),
			)
		}
		result.Docs = result.Docs[from:to]
	}

	// Приведение результатов к нужному типу.
	stringIds := make([]string, len(result.Docs))
	for i, docId := range result.Docs {
		stringIds[i] = strconv.Itoa(int(docId))
	}

	// Приведение статистики фильтров к нужному типу.
	filtersStats := make([]*si.SearchResponse_Filter, 0)
	if len(result.FilterStats) > 0 {
		for filterName, filterValuesStats := range result.FilterStats {
			filterResp := &si.SearchResponse_Filter{
				Name:   string(filterName),
				Values: make([]*si.SearchResponse_Filter_Value, 0),
			}
			for filterValue, resultsNum := range filterValuesStats {
				if resultsNum == 0 {
					continue
				}
				filterResp.Values = append(
					filterResp.Values,
					&si.SearchResponse_Filter_Value{
						Name:  string(filterValue),
						Total: int32(resultsNum),
					},
				)
			}
			filtersStats = append(filtersStats, filterResp)
		}
	}

	return &si.SearchResponse{
		DocIds:         stringIds,
		Filters:        filtersStats,
		SuggestedTerms: result.SuggestedTerms,
	}, nil
}

func (s *IndexerServer) Autocomplete(
	_ context.Context, r *si.AutocompleteRequest,
) (*si.AutocompleteResponse, error) {
	terms, err := s.manager.DoAutocomplete(r.GetService(), r.GetTerm(), int(r.GetLimit()))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &si.AutocompleteResponse{Terms: terms}, nil
}

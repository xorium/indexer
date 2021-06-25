package server

import (
	"bitbucket.org/entrlcom/indexer/core"
	"context"
	"io"
	"log"
	"strings"

	si "bitbucket.org/entrlcom/genproto/gen/go/search/indexer/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (s *IndexerServer) CreateIndex(_ context.Context, r *si.CreateIndexRequest) (*emptypb.Empty,
	error) {
	res := new(emptypb.Empty)

	name := r.GetName()
	if len(name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid index name")
	}

	indexSettings := core.IndexSettings{
		IdentifierField: "id",
		FilteringFields: r.GetSettings().GetFacetedFields(),
		IndexingFields:  r.GetSettings().GetSearchableFields(),
		PreloadFilters:  true,
	}
	if err := s.manager.CreateIndex(name, indexSettings); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return res, nil
}

func (s *IndexerServer) DeleteIndex(_ context.Context, r *si.DeleteIndexRequest) (*emptypb.Empty,
	error) {
	res := new(emptypb.Empty)

	paths := strings.Split(r.GetResourceId(), "/")
	if len(paths) != 2 {
		return nil, status.Error(codes.NotFound, "index not found")
	}

	if err := s.manager.DeleteIndex(paths[1]); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return res, nil
}

func (s *IndexerServer) IndexDocuments(stream si.IndexerService_IndexDocumentsServer) error {
	r, err := stream.Recv()
	if err != nil {
		return status.Error(codes.Unknown, "")
	}

	paths := strings.Split(r.GetResourceId(), "/")
	if len(paths) != 3 {
		return status.Error(codes.NotFound, "invalid index URI")
	}
	indexName := paths[2]
	log.Println("Start creating index", indexName)

	// guard := make(chan struct{}, runtime.NumCPU())
	// concurrentUpdateIndex := func(doc *si.Document, docNum int) {
	// 	guard <- struct{}{}
	// 	go func() {
	// 		log.Println("Start updating index for document", doc.Id)
	// 		if err := s.manager.UpdateIndex(indexName, doc); err != nil {
	// 			log.Printf(
	// 				"error while updating index with document %s: %v\n",
	// 				doc.Id, err,
	// 			)
	// 		}
	// 		log.Printf("Doc #%d, ID %v updated in index\n", docNum, doc.Id)
	// 		<-guard
	// 	}()
	// }

	count := 0
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("Error while reading course message:", err)
			return status.Error(codes.Unknown, "")
		}

		doc := r.GetDocument()
		count++
		//concurrentUpdateIndex(doc, count)
		//log.Println("Start updating index for document", doc.Id)
		if err := s.manager.UpdateIndex(indexName, doc); err != nil {
			log.Printf(
				"error while updating index with document %s: %v\n",
				doc.Id, err,
			)
			continue
		}
		if count%1000 == 0 {
			log.Printf("Doc #%d, ID %v updated in index\n", count, doc.Id)
		}
	}

	log.Println("Finishing index creation...")
	if err := s.manager.FinishCreatingIndex(indexName); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	log.Printf("%d documents have been indexed in %s index.\n", count, indexName)

	if err := stream.SendAndClose(new(emptypb.Empty)); err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	return nil
}

func (s *IndexerServer) ListIndices(
	context.Context, *si.ListIndicesRequest,
) (*si.ListIndicesResponse, error) {
	indicesSettings := s.manager.ListIndices()

	indicesNames := make([]string, 0)
	for indexName := range indicesSettings {
		indicesNames = append(indicesNames, indexName)
	}

	return &si.ListIndicesResponse{Name: indicesNames}, nil
}

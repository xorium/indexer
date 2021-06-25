package server

import (
	"bitbucket.org/entrlcom/indexer/core"
	"context"
	"log"
	"net"
	"net/http"

	is "bitbucket.org/entrlcom/genproto/gen/go/search/indexer/v1"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
)

const dbPath = "./data/index"

type IndexerServer struct {
	manager *core.Indexer
}

func NewIndexerServer(manager *core.Indexer) *IndexerServer {
	return &IndexerServer{
		manager: manager,
	}
}

func StartServer(serverAddr, dbPath string) {
	gs := grpc.NewServer()

	mgr, err := core.NewManager(dbPath, core.NewFileSystemStorage())
	if err != nil {
		panic(err)
	}
	srv := NewIndexerServer(mgr)
	is.RegisterIndexerServiceServer(gs, srv)

	lis, err := net.Listen("tcp", serverAddr)
	if err != nil {
		panic(err)
	}

	log.Println("gRPC server started...")

	if err := gs.Serve(lis); err != nil {
		panic(err)
	}
}

func StartGateway(gatewayAddr, serverAddr string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := runtime.NewServeMux()

	opts := []grpc.DialOption{grpc.WithInsecure()}

	if err := is.RegisterIndexerServiceHandlerFromEndpoint(ctx, mux, serverAddr, opts); err != nil {
		panic(err)
	}

	log.Println("gRPC gateway started...")

	if err := http.ListenAndServe(gatewayAddr, mux); err != nil {
		panic(err)
	}
}

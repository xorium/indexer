package main

import (
	"flag"
	"os"

	"bitbucket.org/entrlcom/indexer/server"
)

func main() {
	serverAddr := flag.String("server_addr", ":9000", "")
	gatewayAddr := flag.String("gateway_addr", ":9443", "")
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "/opt/indexer"
	}
	flag.Parse()

	go server.StartServer(*serverAddr, dbPath)
	go server.StartGateway(*gatewayAddr, *serverAddr)

	select {}
}

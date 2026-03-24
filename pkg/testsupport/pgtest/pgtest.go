package pgtest

import (
	"fmt"
	"net"
	"path/filepath"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

type EmbeddedServer struct {
	db *embeddedpostgres.EmbeddedPostgres

	Host     string
	Port     int
	User     string
	Password string
	Database string
}

func StartEmbedded(baseDir string) (*EmbeddedServer, error) {
	port, err := freeTCPPort()
	if err != nil {
		return nil, err
	}
	cfg := embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.V18).
		Port(uint32(port)).
		RuntimePath(filepath.Join(baseDir, "runtime")).
		CachePath(filepath.Join(baseDir, "cache")).
		DataPath(filepath.Join(baseDir, "data"))

	db := embeddedpostgres.NewDatabase(cfg)
	if err := db.Start(); err != nil {
		return nil, fmt.Errorf("start embedded postgres: %w", err)
	}
	return &EmbeddedServer{
		db:       db,
		Host:     "localhost",
		Port:     port,
		User:     "postgres",
		Password: "postgres",
		Database: "postgres",
	}, nil
}

func (s *EmbeddedServer) Stop() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Stop()
}

func freeTCPPort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("allocate free tcp port: %w", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

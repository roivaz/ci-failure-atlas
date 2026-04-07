package options

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"ci-failure-atlas/pkg/store/postgres/initdb"
	"ci-failure-atlas/pkg/store/postgres/migrations"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
)

func DefaultOptions() *RawOptions {
	return &RawOptions{
		Enabled:               false,
		Embedded:              false,
		EmbeddedDataDir:       "data/postgres",
		EmbeddedBinaryDir:     "",
		Hostname:              "",
		Port:                  5432,
		User:                  "",
		Password:              "",
		Database:              "",
		SSLMode:               "disable",
		MaxConnections:        20,
		MinConnections:        1,
		ConnectTimeoutSeconds: 15,
		Initialize:            false,
	}
}

func DefaultCLIOptions() *RawOptions {
	raw := DefaultOptions()
	raw.Enabled = true
	raw.Embedded = true
	raw.Initialize = true
	return raw
}

func BindOptions(opts *RawOptions, cmd *cobra.Command) error {
	cmd.Flags().BoolVar(&opts.Enabled, "storage.postgres.enabled", opts.Enabled, "Enable PostgreSQL storage backend.")
	cmd.Flags().BoolVar(&opts.Embedded, "storage.postgres.embedded", opts.Embedded, "Run an embedded Postgres database instead of connecting to a remote instance.")
	cmd.Flags().StringVar(&opts.EmbeddedDataDir, "storage.postgres.embedded.data-dir", opts.EmbeddedDataDir, "Directory where embedded Postgres stores data.")
	cmd.Flags().StringVar(&opts.EmbeddedBinaryDir, "storage.postgres.embedded.binary-dir", opts.EmbeddedBinaryDir, "Directory containing PostgreSQL binaries for embedded mode.")
	cmd.Flags().StringVar(&opts.Hostname, "storage.postgres.host", opts.Hostname, "Remote PostgreSQL host name.")
	cmd.Flags().IntVar(&opts.Port, "storage.postgres.port", opts.Port, "Remote PostgreSQL host port.")
	cmd.Flags().StringVar(&opts.User, "storage.postgres.user", opts.User, "Remote PostgreSQL user.")
	cmd.Flags().StringVar(&opts.Password, "storage.postgres.password", opts.Password, "Remote PostgreSQL password.")
	cmd.Flags().StringVar(&opts.Database, "storage.postgres.database", opts.Database, "Remote PostgreSQL database name.")
	cmd.Flags().StringVar(&opts.SSLMode, "storage.postgres.sslmode", opts.SSLMode, "PostgreSQL sslmode (disable, require, verify-ca, verify-full).")
	cmd.Flags().IntVar(&opts.MaxConnections, "storage.postgres.max-connections", opts.MaxConnections, "Maximum PostgreSQL pooled connections.")
	cmd.Flags().IntVar(&opts.MinConnections, "storage.postgres.min-connections", opts.MinConnections, "Minimum PostgreSQL pooled connections.")
	cmd.Flags().IntVar(&opts.ConnectTimeoutSeconds, "storage.postgres.connect-timeout-seconds", opts.ConnectTimeoutSeconds, "PostgreSQL connect timeout in seconds.")
	cmd.Flags().BoolVar(&opts.Initialize, "storage.postgres.initialize", opts.Initialize, "Initialize schema and run embedded SQL migrations on startup.")
	return nil
}

type RawOptions struct {
	Enabled               bool
	Embedded              bool
	EmbeddedDataDir       string
	EmbeddedBinaryDir     string
	Hostname              string
	Port                  int
	User                  string
	Password              string
	Database              string
	SSLMode               string
	MaxConnections        int
	MinConnections        int
	ConnectTimeoutSeconds int
	Initialize            bool
}

type validatedOptions struct {
	*RawOptions
	EmbeddedDataDir   string
	EmbeddedBinaryDir string
	Hostname          string
	User              string
	Password          string
	Database          string
	SSLMode           string
}

type ValidatedOptions struct {
	*validatedOptions
}

type completedOptions struct {
	Enabled       bool
	Embedded      bool
	ConnectionURL string
	Connection    *pgxpool.Pool
	cleanup       func()
}

type Options struct {
	*completedOptions
}

func (o *RawOptions) Validate() (*ValidatedOptions, error) {
	if o == nil {
		return nil, fmt.Errorf("postgres options are required")
	}

	embeddedDataDir := strings.TrimSpace(o.EmbeddedDataDir)
	embeddedBinaryDir := strings.TrimSpace(o.EmbeddedBinaryDir)
	hostname := strings.TrimSpace(o.Hostname)
	user := strings.TrimSpace(o.User)
	password := strings.TrimSpace(o.Password)
	database := strings.TrimSpace(o.Database)
	sslMode := strings.ToLower(strings.TrimSpace(o.SSLMode))
	if sslMode == "" {
		sslMode = "disable"
	}
	if !isValidSSLMode(sslMode) {
		return nil, fmt.Errorf("invalid --storage.postgres.sslmode value %q", o.SSLMode)
	}
	if o.Port <= 0 || o.Port > 65535 {
		return nil, fmt.Errorf("--storage.postgres.port must be between 1 and 65535")
	}
	if o.MaxConnections <= 0 {
		return nil, fmt.Errorf("--storage.postgres.max-connections must be greater than 0")
	}
	if o.MinConnections <= 0 {
		return nil, fmt.Errorf("--storage.postgres.min-connections must be greater than 0")
	}
	if o.MinConnections > o.MaxConnections {
		return nil, fmt.Errorf("--storage.postgres.min-connections must be <= --storage.postgres.max-connections")
	}
	if o.ConnectTimeoutSeconds <= 0 {
		return nil, fmt.Errorf("--storage.postgres.connect-timeout-seconds must be greater than 0")
	}

	if o.Enabled {
		if o.Embedded {
			if embeddedDataDir == "" {
				return nil, fmt.Errorf("--storage.postgres.embedded.data-dir must be provided when --storage.postgres.embedded is set")
			}
		} else {
			if hostname == "" {
				return nil, fmt.Errorf("--storage.postgres.host must be provided when postgres storage is enabled in remote mode")
			}
			if user == "" {
				return nil, fmt.Errorf("--storage.postgres.user must be provided when postgres storage is enabled in remote mode")
			}
			if database == "" {
				return nil, fmt.Errorf("--storage.postgres.database must be provided when postgres storage is enabled in remote mode")
			}
		}
	}

	return &ValidatedOptions{
		validatedOptions: &validatedOptions{
			RawOptions:        o,
			EmbeddedDataDir:   embeddedDataDir,
			EmbeddedBinaryDir: embeddedBinaryDir,
			Hostname:          hostname,
			User:              user,
			Password:          password,
			Database:          database,
			SSLMode:           sslMode,
		},
	}, nil
}

func (o *ValidatedOptions) Complete(ctx context.Context) (*Options, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	if o == nil || o.validatedOptions == nil {
		return nil, fmt.Errorf("validated postgres options are required")
	}

	if !o.Enabled {
		return &Options{
			completedOptions: &completedOptions{
				Enabled:       false,
				Embedded:      false,
				ConnectionURL: "",
				Connection:    nil,
				cleanup:       func() {},
			},
		}, nil
	}

	cleanup := func() {}
	var connectionURL string
	if o.Embedded {
		cfg := embeddedpostgres.DefaultConfig().
			Version(embeddedpostgres.V18).
			DataPath(o.EmbeddedDataDir)
		if o.EmbeddedBinaryDir != "" {
			cfg = cfg.BinariesPath(o.EmbeddedBinaryDir)
		}

		db := embeddedpostgres.NewDatabase(cfg)
		if err := db.Start(); err != nil {
			return nil, fmt.Errorf("start embedded postgres: %w", err)
		}
		cleanup = func() {
			_ = db.Stop()
		}
		connectionURL = cfg.GetConnectionURL()
	} else {
		var err error
		connectionURL, err = buildConnectionURL(o.User, o.Password, o.Hostname, o.Port, o.Database, o.SSLMode)
		if err != nil {
			return nil, err
		}
	}

	poolConfig, err := pgxpool.ParseConfig(connectionURL)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("parse postgres connection URL: %w", err)
	}
	poolConfig.MaxConns = int32(o.MaxConnections)
	poolConfig.MinConns = int32(o.MinConnections)
	poolConfig.ConnConfig.ConnectTimeout = time.Duration(o.ConnectTimeoutSeconds) * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		cleanup()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	fullCleanup := func() {
		pool.Close()
		cleanup()
	}

	if o.Initialize {
		if err := initdb.Initialize(ctx, pool); err != nil {
			fullCleanup()
			return nil, err
		}
		if err := migrations.Run(ctx, pool); err != nil {
			fullCleanup()
			return nil, err
		}
	}

	return &Options{
		completedOptions: &completedOptions{
			Enabled:       true,
			Embedded:      o.Embedded,
			ConnectionURL: connectionURL,
			Connection:    pool,
			cleanup:       fullCleanup,
		},
	}, nil
}

func (o *Options) Cleanup() {
	if o == nil || o.completedOptions == nil || o.cleanup == nil {
		return
	}
	o.cleanup()
}

func isValidSSLMode(mode string) bool {
	switch mode {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		return true
	default:
		return false
	}
}

func buildConnectionURL(user, password, host string, port int, database, sslMode string) (string, error) {
	if host == "" || user == "" || database == "" {
		return "", fmt.Errorf("build postgres connection URL requires host, user, and database")
	}

	u := &url.URL{
		Scheme: "postgres",
		Host:   host + ":" + strconv.Itoa(port),
		Path:   database,
	}
	if password == "" {
		u.User = url.User(user)
	} else {
		u.User = url.UserPassword(user, password)
	}

	query := url.Values{}
	query.Set("sslmode", sslMode)
	u.RawQuery = query.Encode()
	return u.String(), nil
}

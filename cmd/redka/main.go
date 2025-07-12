// Redka server.
// Example usage:
//
//	./redka -h localhost -p 6379 redka.db
// 	./redka -h localhost -p 6379 "postgres://redka:redka@localhost:5432/redka?sslmode=disable"

package main

import (
	"cmp"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	_ "github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/server"
)

// set by the build process
var (
	version = "main"
	commit  = "none"
	date    = "unknown"
)

const debugPort = 6060
const sqliteDriverName = "sqlite-redka"
const sqliteMemoryURI = "file:/redka.db?vfs=memdb"
const sqlitePragma = `
pragma journal_mode = wal;
pragma synchronous = normal;
pragma temp_store = memory;
pragma mmap_size = 268435456;
pragma foreign_keys = on;`

// Config holds the server configuration.
type Config struct {
	Host    string
	Port    string
	Sock    string // unix socket
	Path    string
	Verbose bool
}

func (c *Config) Addr() string {
	return net.JoinHostPort(c.Host, c.Port)
}

func init() {
	// Set up flag usage message.
	flag.Usage = func() {
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "Usage: redka [options] <data-source>\n")
		flag.PrintDefaults()
	}

	// Register an SQLite driver with custom pragmas.
	// Ensures that the PRAGMA settings apply to
	// all connections opened by the driver.
	sql.Register(sqliteDriverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			_, err := conn.Exec(sqlitePragma, nil)
			return err
		},
	})
}

func main() {
	config := mustReadConfig()
	logger := setupLogger(config)

	slog.Info("starting redka", "version", version, "commit", commit, "built_at", date)

	// Prepare a context to handle shutdown signals.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Open the database.
	db := mustOpenDB(config, logger)

	// Start application and debug servers.
	errCh := make(chan error, 1)
	srv := startServer(config, db, errCh)
	debugSrv := startDebugServer(config, errCh)

	// Wait for a shutdown signal or a startup error.
	select {
	case <-ctx.Done():
		shutdown(srv, debugSrv)
	case err := <-errCh:
		slog.Error("startup", "error", err)
		shutdown(srv, debugSrv)
		os.Exit(1)
	}
}

// mustReadConfig reads the configuration from
// command line arguments and environment variables.
func mustReadConfig() Config {
	if len(flag.Args()) > 1 {
		flag.Usage()
		os.Exit(1)
	}

	var config Config
	flag.StringVar(
		&config.Host, "h",
		cmp.Or(os.Getenv("REDKA_HOST"), "localhost"),
		"server host",
	)
	flag.StringVar(
		&config.Port, "p",
		cmp.Or(os.Getenv("REDKA_PORT"), "6379"),
		"server port",
	)
	flag.StringVar(
		&config.Sock, "s",
		cmp.Or(os.Getenv("REDKA_SOCK"), ""),
		"server socket (overrides host and port)",
	)
	flag.BoolVar(&config.Verbose, "v", false, "verbose logging")
	flag.Parse()

	config.Path = cmp.Or(flag.Arg(0), os.Getenv("REDKA_DB_URL"), sqliteMemoryURI)
	return config
}

// setupLogger setups a logger for the application.
func setupLogger(config Config) *slog.Logger {
	logLevel := new(slog.LevelVar)
	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)
	if config.Verbose {
		logLevel.Set(slog.LevelDebug)
	}
	return logger
}

// mustOpenDB connects to the database.
func mustOpenDB(config Config, logger *slog.Logger) *redka.DB {
	// Connect to the database using the inferred driver.
	driverName := inferDriverName(config.Path)
	opts := redka.Options{
		DriverName: driverName,
		Logger:     logger,
		// Using nil for pragma sets the default options.
		// We don't want any options, so pass an empty map instead.
		Pragma: map[string]string{},
	}
	db, err := redka.Open(config.Path, &opts)
	if err != nil {
		slog.Error("data source", "error", err)
		os.Exit(1)
	}

	// Hide password when logging.
	maskedPath := config.Path
	if u, err := url.Parse(maskedPath); err == nil && u.User != nil {
		u.User = url.User(u.User.Username())
		maskedPath = u.String()
	}
	slog.Info("data source", "driver", driverName, "path", maskedPath)

	return db
}

// inferDriverName infers the driver name from the data source URI.
func inferDriverName(path string) string {
	// Infer the driver name based on the data source URI.
	if strings.HasPrefix(path, "postgres://") {
		return "postgres"
	}
	return sqliteDriverName
}

// startServer starts the application server.
func startServer(config Config, db *redka.DB, errCh chan<- error) *server.Server {
	// Create the server.
	var srv *server.Server
	if config.Sock != "" {
		srv = server.New("unix", config.Sock, db)
	} else {
		srv = server.New("tcp", config.Addr(), db)
	}

	// Start the server.
	go func() {
		if err := srv.Start(); err != nil {
			errCh <- fmt.Errorf("start server: %w", err)
		}
	}()

	return srv
}

// startDebugServer starts the debug server.
func startDebugServer(config Config, errCh chan<- error) *server.DebugServer {
	if !config.Verbose {
		return nil
	}
	srv := server.NewDebug("localhost", debugPort)
	go func() {
		if err := srv.Start(); err != nil {
			errCh <- fmt.Errorf("start debug server: %w", err)
		}
	}()
	return srv
}

// shutdown stops the main server and the debug server.
func shutdown(srv *server.Server, debugSrv *server.DebugServer) {
	// Stop the debug server.
	if debugSrv != nil {
		if err := debugSrv.Stop(); err != nil {
			slog.Error("stop debug server", "error", err)
		}
		slog.Info("stop debug server")
	}

	// Stop the main server.
	if err := srv.Stop(); err != nil {
		slog.Error("stop server", "error", err)
	}
	slog.Info("stop server")
}

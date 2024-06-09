// Redka server.
// Example usage:
//
//	./redka -h localhost -p 6379 redka.db
//
// Example usage (client):
//
//	docker run --rm -it redis redis-cli -h host.docker.internal -p 6379
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

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

const driverName = "redka"
const memoryURI = "file:redka?mode=memory&cache=shared"
const pragma = `
pragma journal_mode = wal;
pragma synchronous = normal;
pragma temp_store = memory;
pragma mmap_size = 268435456;
pragma foreign_keys = on;`

// Config holds the server configuration.
type Config struct {
	Host    string
	Port    string
	Path    string
	Verbose bool
}

func (c *Config) Addr() string {
	return net.JoinHostPort(c.Host, c.Port)
}

var config Config

func init() {
	// Set up command line flags.
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: redka [options] <data-source>\n")
		flag.PrintDefaults()
	}
	flag.StringVar(&config.Host, "h", "localhost", "server host")
	flag.StringVar(&config.Port, "p", "6379", "server port")
	flag.BoolVar(&config.Verbose, "v", false, "verbose logging")

	// Register an SQLite driver with custom pragmas.
	// Ensures that the PRAGMA settings apply to
	// all connections opened by the driver.
	sql.Register(driverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			_, err := conn.Exec(pragma, nil)
			return err
		},
	})
}

func main() {
	// Parse command line arguments.
	flag.Parse()
	if len(flag.Args()) > 1 {
		flag.Usage()
		os.Exit(1)
	}

	// Set the data source.
	if len(flag.Args()) == 0 {
		config.Path = memoryURI
	} else {
		config.Path = flag.Arg(0)
	}

	// Prepare a context to handle shutdown signals.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Set up logging.
	logLevel := new(slog.LevelVar)
	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)
	if config.Verbose {
		logLevel.Set(slog.LevelDebug)
	}

	// Print version information.
	slog.Info("starting redka", "version", version, "commit", commit, "built_at", date)

	// Open the database.
	opts := redka.Options{
		DriverName: driverName,
		Logger:     logger,
		Pragma:     map[string]string{},
	}
	db, err := redka.Open(config.Path, &opts)
	if err != nil {
		slog.Error("data source", "error", err)
		os.Exit(1)
	}
	slog.Info("data source", "path", config.Path)

	// Start the server.
	srv := server.New(config.Addr(), db)
	srv.Start()

	// Wait for a shutdown signal.
	<-ctx.Done()

	// Stop the server.
	if err := srv.Stop(); err != nil {
		slog.Error("stop server", "error", err)
	}
	slog.Info("stop server")
}

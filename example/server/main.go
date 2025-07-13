// An example of running a Redka server
// within your own application.
package main

import (
	"context"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/redsrv"
	"github.com/redis/go-redis/v9"
)

func main() {
	// Start a Redka server with an in-memory database.
	db := mustOpen()
	srv := mustStart(db)
	defer func() {
		_ = srv.Stop()
		fmt.Println("redka server stopped")
	}()
	fmt.Println("redka server started")

	// The server is now running and ready to accept connections.
	// You can use the regular go-redis package to access Redka.
	rdb := redis.NewClient(&redis.Options{Addr: ":6380"})
	defer func() { _ = rdb.Close() }()

	ctx := context.Background()
	rdb.Set(ctx, "name", "alice", 0)
	rdb.Set(ctx, "age", 25, 0)

	name, _ := rdb.Get(ctx, "name").Result()
	fmt.Println("name =", name)
	age, _ := rdb.Get(ctx, "age").Int()
	fmt.Println("age =", age)
}

// mustOpen opens an in-memory Redka database.
func mustOpen() *redka.DB {
	db, err := redka.Open("file:/redka.db?vfs=memdb", nil)
	if err != nil {
		panic(err)
	}
	return db
}

// mustStart starts a Redka server on localhost:6380.
func mustStart(db *redka.DB) *redsrv.Server {
	srv := redsrv.New("tcp", ":6380", db)

	// The ready channel will receive a nil value when the server is ready,
	// or an error if it fails to start.
	ready := make(chan error, 1)
	go func() {
		if err := srv.Start(ready); err != nil {
			ready <- err
			return
		}
	}()

	// Wait for the server to be ready.
	if err := <-ready; err != nil {
		panic(err)
	}
	return srv
}

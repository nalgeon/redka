# Using Redka as a standalone server

Redka server is a single-file binary. After [downloading and unpacking](install-standalone.md) the release asset, run it as follows:

```
redka [-h host] [-p port] [-s unix-socket] [db-path]
```

For example:

```shell
# use in-memory sqlite database
./redka

# use file sqlite database
./redka data.db

# listen on all network interfaces
./redka -h 0.0.0.0 -p 6379 data.db

# listen on unix socket
./redka -s /tmp/redka.sock data.db

# use postgres database
./redka -p 6379 "postgres://redka:redka@localhost:5432/redka?sslmode=disable"
```

Server defaults are host `localhost`, port `6379` and empty DB path. The unix socket path, if given, overrides the host/port arguments.

Running without a DB path creates an in-memory database. The data is not persisted in this case, and will be gone when the server is stopped.

You can also run Redka with Docker as follows:

```shell
# database inside the container
# will be lost when the container stops
docker run --rm -p 6379:6379 nalgeon/redka

# persistent database
# using the /path/to/data host directory
docker run --rm -p 6379:6379 -v /path/to/data:/data nalgeon/redka

# in-memory database, custom post
docker run --rm -p 6380:6380 nalgeon/redka redka -h 0.0.0.0 -p 6380
```

Server defaults in Docker are host `0.0.0.0`, port `6379` and DB path `/data/redka.db`.

Once the server is running, connect to it using `redis-cli` or an API client like `redis-py` or `go-redis` — just as you would with Redis.

```shell
redis-cli -h localhost -p 6379
```

```text
127.0.0.1:6379> echo hello
"hello"
127.0.0.1:6379> set name alice
OK
127.0.0.1:6379> get name
"alice"
```

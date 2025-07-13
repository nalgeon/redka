# Using Redka as a standalone server

Redka server is a single-file binary. After [downloading and unpacking](install-standalone.md) the release asset, run it as follows:

```
redka [-h host] [-p port] [-s unix-socket] [db-path]
```

For example:

```shell
# Use in-memory sqlite database.
./redka

# Use file sqlite database.
./redka redka.db

# Listen on all network interfaces.
./redka -h 0.0.0.0 -p 6379 redka.db

# Listen on unix socket.
./redka -s /tmp/redka.sock redka.db

# Use postgres database.
./redka -p 6379 "postgres://redka:redka@localhost:5432/redka?sslmode=disable"
```

Server defaults are host `localhost`, port `6379` and empty DB path. The unix socket path, if given, overrides the host/port arguments.

Running without a DB path creates an in-memory database. The data is not persisted in this case, and will be gone when the server is stopped.

You can also run Redka with Docker as follows:

```shell
# In-memory sqlite database.
docker run --rm -p 6379:6379 nalgeon/redka

# Persistent sqlite database
# using the /path/to/data host directory.
docker run --rm -p 6379:6379 -v /path/to/data:/data nalgeon/redka redka.db

# Postgres database on host machine.
docker run --rm -p 6379:6379 nalgeon/redka "postgres://redka:redka@host.docker.internal:5432/redka?sslmode=disable"
```

Server defaults in Docker are host `0.0.0.0`, port `6379` and empty DB path.

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

Redka aims to reimplement the good parts of Redis with SQLite, while remaining compatible with Redis protocol.

Notable features:

-   Data does not have to fit in RAM.
-   ACID transactions.
-   SQL views for better introspection and reporting.
-   Both in-process (Go API) and standalone (RESP) servers.
-   Redis-compatible commands and wire protocol.

This is a work in progress. See below for the current status and roadmap.

[Commands](#commands) •
[Installation](#installation) •
[Usage](#usage) •
[Persistence](#persistence) •
[Performance](#performance) •
[Roadmap](#roadmap) •
[More](#more-information)

## Commands

Redka aims to support five core Redis data types: strings, lists, sets, hashes, and sorted sets.

### Strings

Strings are the most basic Redis type, representing a sequence of bytes. Redka supports the following string-related commands:

| Command       | Go API                  | Description                                                           |
| ------------- | ----------------------- | --------------------------------------------------------------------- |
| `APPEND`      | `DB.Str().Append`       | Appends a string to the value of a key.                               |
| `DECR`        | `DB.Str().Incr`         | Decrements the integer value of a key by one.                         |
| `DECRBY`      | `DB.Str().Incr`         | Decrements a number from the integer value of a key.                  |
| `GET`         | `DB.Str().Get`          | Returns the string value of a key.                                    |
| `GETRANGE`    | `DB.Str().GetRange`     | Returns a substring of the string stored at a key.                    |
| `GETSET`      | `DB.Str().GetSet`       | Sets the key to a new value and returns the prev value.               |
| `INCR`        | `DB.Str().Incr`         | Increments the integer value of a key by one.                         |
| `INCRBY`      | `DB.Str().Incr`         | Increments the integer value of a key by a number.                    |
| `INCRBYFLOAT` | `DB.Str().Incr`         | Increments the float value of a key by a number.                      |
| `MGET`        | `DB.Str().GetMany`      | Returns the string values of one or more keys.                        |
| `MSET`        | `DB.Str().SetMany`      | Sets the string values of one or more keys.                           |
| `MSETNX`      | `DB.Str().SetManyNX`    | Sets the string values of one or more keys when all keys don't exist. |
| `PSETEX`      | `DB.Str().Set`          | Sets the string value and expiration time (in ms) of a key.           |
| `SET`         | `DB.Str().Set`          | Sets the string value of a key.                                       |
| `SETEX`       | `DB.Str().SetExpires`   | Sets the string value and expiration (in sec) time of a key.          |
| `SETNX`       | `DB.Str().SetNotExists` | Sets the string value of a key when the key doesn't exist.            |
| `SETRANGE`    | `DB.Str().SetRange`     | Overwrites a part of a string value with another by an offset.        |
| `STRLEN`      | `DB.Str().Length`       | Returns the length of a string value.                                 |
| `SUBSTR`      | `DB.Str().GetRange`     | Same as `GETRANGE`.                                                   |

The following string-related commands are not planned for 1.0:

```
GETDEL  GETEX  LCS
```

### Lists

Lists are lists of strings sorted by insertion order. Redka aims to support the following list-related commands in 1.0:

```
LINDEX  LINSERT  LLEN  LPOP  LPUSHX  LRANGE  LREM  LSET
LTRIM  RPOP  RPOPLPUSH  RPUSH  RPUSHX
```

### Sets

Sets are unordered collections of unique strings. Redka aims to support the following set-related commands in 1.0:

```
SADD  SCARD  SDIFF  SDIFFSTORE  SINTER  SINTERSTORE
SISMEMBER  SMEMBERS  SMOVE  SPOP  SRANDMEMBER  SREM
SUNION  SUNIONSTORE
```

### Hashes

Hashes are record types modeled as collections of field-value pairs. Redka aims to support the following hash-related commands in 1.0:

```
HDEL  HEXISTS  HGET  HGETALL  HINCRBY  HINCRBYFLOAT  HKEYS
HLEN  HMGET  HMSET  HSET  HSETNX  HVALS
```

### Sorted sets

Sorted sets are collections of unique strings ordered by each string's associated score. Redka aims to support the following sorted set related commands in 1.0:

```
ZADD  ZCARD  ZCOUNT  ZINCRBY  ZINTERSTORE  ZRANGE
ZRANK  ZREM  ZSCORE
```

### Key management

Redka supports the following key management (generic) commands:

| Command     | Go API              | Description                                                |
| ----------- | ------------------- | ---------------------------------------------------------- |
| `DEL`       | `DB.Key().Delete`   | Deletes one or more keys.                                  |
| `EXISTS`    | `DB.Key().Exists`   | Determines whether one or more keys exist.                 |
| `EXPIRE`    | `DB.Key().Expire`   | Sets the expiration time of a key (in seconds).            |
| `EXPIREAT`  | `DB.Key().ExpireAt` | Sets the expiration time of a key to a Unix timestamp.     |
| `KEYS`      | `DB.Key().Search`   | Returns all key names that match a pattern.                |
| `PERSIST`   | `DB.Key().Persist`  | Removes the expiration time of a key.                      |
| `PEXPIRE`   | `DB.Key().Expire`   | Sets the expiration time of a key in ms.                   |
| `PEXPIREAT` | `DB.Key().ExpireAt` | Sets the expiration time of a key to a Unix ms timestamp.  |
| `RANDOMKEY` | `DB.Key().Random`   | Returns a random key name from the database.               |
| `RENAME`    | `DB.Key().Rename`   | Renames a key and overwrites the destination.              |
| `RENAMENX`  | `DB.Key().RenameNX` | Renames a key only when the target key name doesn't exist. |
| `SCAN`      | `DB.Key().Scanner`  | Iterates over the key names in the database.               |

The following generic commands are not planned for 1.0:

```
COPY  DUMP  EXPIRETIME  MIGRATE  MOVE  OBJECT  PEXPIRETIME
PTTL  RESTORE  SORT  SORT_RO  TOUCH  TTL  TYPE  UNLINK
WAIT  WAITAOF
```

### Transactions

Redka supports the following transaction commands:

| Command   | Go API                  | Description                             |
| --------- | ----------------------- | --------------------------------------- |
| `DISCARD` | `DB.View` / `DB.Update` | Discards a transaction.                 |
| `EXEC`    | `DB.View` / `DB.Update` | Executes all commands in a transaction. |
| `MULTI`   | `DB.View` / `DB.Update` | Starts a transaction.                   |

Unlike Redis, Redka's transactions are fully ACID, providing automatic rollback in case of failure.

The following transaction commands are not planned for 1.0:

```
UNWATCH  WATCH
```

### Server/connection management

Redka supports only a couple of server and connection management commands:

| Command   | Go API     | Description                        |
| --------- | ---------- | ---------------------------------- |
| `ECHO`    | —          | Returns the given string.          |
| `FLUSHDB` | `DB.Flush` | Remove all keys from the database. |

The rest of the server and connection management commands are not planned for 1.0.

## Installation

Redka can be installed as a standalone Redis-compatible server, or as a Go module for in-process use.

## Standalone server

Redka server is a single-file binary. Download it from the [releases](https://github.com/nalgeon/redka/releases).

Linux (x86 CPU only):

```shell
curl -L -O "https://github.com/nalgeon/redka/releases/download/0.1.0/redka_linux_amd64.zip"
unzip redka_linux_amd64.zip
chmod +x redka
```

macOS (both x86 and ARM/Apple Silicon CPU):

```shell
curl -L -O "https://github.com/nalgeon/redka/releases/download/0.1.0/redka_darwin_amd64.zip"
unzip redka_darwin_amd64.zip
# remove the build from quarantine
# (macOS disables unsigned binaries)
xattr -d com.apple.quarantine redka
chmod +x redka
```

Or pull with Docker as follows:

```shell
docker pull nalgeon/redka
```

Or build from source (requires Go 1.22 and GCC):

```shell
git clone https://github.com/nalgeon/redka.git
cd redka
make setup build
# the path to the binary after the build
# will be ./build/redka
```

## Go module

Install the module as follows:

```shell
go get github.com/nalgeon/redka
```

You'll also need an SQLite driver. Use `github.com/mattn/go-sqlite3` if you don't mind CGO. Otherwise use a pure Go driver `modernc.org/sqlite`. Install either with `go get` like this:

```shell
go get github.com/mattn/go-sqlite3
```

## Usage

Redka can be used as a standalone Redis-compatible server, or as an embeddable in-process server with Go API.

### Standalone server

Redka server is a single-file binary. After downloading and unpacking the release asset, run it as follows:

```
redka [-h host] [-p port] [db-path]
```

For example:

```shell
./redka
./redka data.db
./redka -h localhost -p 6379 data.db
```

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

Note that running in a container may result in poorer performance.

Once the server is running, connect to it using `redis-cli` or an API client like `redis-py` or `go-redis` — just as you would with Redis.

```shell
redis-cli -h localhost -p 6379
```

```
127.0.0.1:6379> echo hello
"hello"
127.0.0.1:6379> set name alice
OK
127.0.0.1:6379> get name
"alice"
```

### In-process server

The primary object in Redka is the `DB`. To open or create your database, use the `redka.Open()` function:

```go
package main

import (
    "log"

    _ "github.com/mattn/go-sqlite3"
    "github.com/nalgeon/redka"
)

func main() {
    // Open the data.db file. It will be created if it doesn't exist.
    db, err := redka.Open("data.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    // ...
}
```

Don't forget to import the driver (here I use `github.com/mattn/go-sqlite3`). Using `modernc.org/sqlite` is slightly different, see `example/modernc/main.go` for details.

To open an in-memory database that doesn't persist to disk, use `:memory:` as the path to the file:

```go
redka.Open(":memory:") // All data is lost when the database is closed.
```

After opening the database, call `redka.DB` methods to run individual commands:

```go
db.Str().Set("name", "alice")
db.Str().Set("age", 25)

count, err := db.Key().Exists("name", "age", "city")
slog.Info("exists", "count", count, "err", err)

name, err := db.Str().Get("name")
slog.Info("get", "name", name, "err", err)
```

```
exists count=2 err=<nil>
get name="alice" err=<nil>
```

See the full example in `example/simple/main.go`.

Use transactions to batch commands. There are `View` (read-only transaction) and `Update` (writable transaction) methods for this:

```go
updCount := 0
err := db.Update(func(tx *redka.Tx) error {
    err := tx.Str().Set("name", "bob")
    if err != nil {
        return err
    }
    updCount++

    err = tx.Str().Set("age", 50)
    if err != nil {
        return err
    }
    updCount++
    return nil
})
slog.Info("updated", "count", updCount, "err", err)
```

```
updated count=2 err=<nil>
```

See the full example in `example/tx/main.go`.

## Persistence

Redka stores data in a SQLite database using the following tables:

```
rkey
---
id       integer primary key
key      text not null
type     integer not null
version  integer not null
etime    integer
mtime    integer not null

rstring
---
key_id   integer
value    blob not null
```

To access the data with SQL, use views instead of tables:

```sql
select * from vstring;
```

```
┌────────┬──────┬───────┬────────┬───────┬─────────────────────┐
│ key_id │ key  │ value │  type  │ etime │        mtime        │
├────────┼──────┼───────┼────────┼───────┼─────────────────────┤
│ 1      │ name │ alice │ string │       │ 2024-04-03 16:58:14 │
│ 2      │ age  │ 50    │ string │       │ 2024-04-03 16:34:52 │
└────────┴──────┴───────┴────────┴───────┴─────────────────────┘
```

Type in views is the Redis data type. Times are in UTC.

## Performance

I've compared Redka with Redis using [redis-benchmark](https://redis.io/docs/management/optimization/benchmarks/) with the following parameters:

-   10 parallel connections
-   1000000 requests
-   10000 randomized keys
-   GET/SET commands

SQLite settings:

```
pragma journal_mode = wal;
pragma synchronous = normal;
pragma temp_store = memory;
pragma mmap_size = 268435456;
pragma foreign_keys = on;
```

Hardware: Apple M1 8-core CPU, 16GB RAM

Results:

```
redis-server --appendonly no
redis-benchmark -p 6379 -q -c 10 -n 1000000 -r 10000 -t get,set

SET: 133262.25 requests per second, p50=0.055 msec
GET: 139217.59 requests per second, p50=0.055 msec
```

```
./redka -p 6380 data.db
redis-benchmark -p 6380 -q -c 10 -n 1000000 -r 10000 -t get,set

SET: 22551.47 requests per second, p50=0.255 msec
GET: 56802.05 requests per second, p50=0.119 msec
```

So while Redka is 2-6 times slower than Redis (not surprising, since we are comparing a relational database to a key-value data store), it can still do 23K writes/sec and 57K reads/sec, which is pretty good if you ask me.

## Roadmap

The project is on its way to 1.0.

The 1.0 release will include the following features from Redis 2.x (which I consider the "golden age" of the Redis API):

-   Strings, lists, sets, hashes and sorted sets.
-   Publish/subscribe.
-   Key management.
-   Transactions.

Future versions may include data types from later Redis versions (such as streams, HyperLogLog or geo) and more commands for existing types.

Features I'd rather not implement even in future versions:

-   Lua scripting.
-   Authentication and ACLs.
-   Multiple databases.
-   Watch/unwatch.

Features I definitely don't want to implement:

-   Cluster.
-   Sentinel.

## More information

### Contributing

Contributions are welcome. For anything other than bugfixes, please first open an issue to discuss what you want to change.

Be sure to add or update tests as appropriate.

### Acknowledgements

Redka would not be possible without these great projects and their creators:

-   [Redis](https://redis.io/) ([Salvatore Sanfilippo](https://github.com/antirez)). It's such an amazing idea to go beyond the get-set paradigm and provide a convenient API for more complex data structures.
-   [SQLite](https://sqlite.org/) ([D. Richard Hipp](https://www.sqlite.org/crew.html)). The in-process database powering the world.
-   [Redcon](https://github.com/tidwall/redcon) ([Josh Baker](https://github.com/tidwall)). A very clean and convenient implementation of a RESP server.

### License

Copyright 2024 [Anton Zhiyanov](https://antonz.org/).

The software is available under the BSD-3-Clause license.

### Stay tuned

★ [Subscribe](https://antonz.org/subscribe/) to stay on top of new features.

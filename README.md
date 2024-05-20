<img alt="Redka" src="logo.svg" height="80" align="center">

Redka aims to reimplement the good parts of Redis with SQLite, while remaining compatible with Redis API.

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

```
Command      Go API                 Description
-------      ------                 -----------
DECR         DB.Str().Incr          Decrements the integer value of a key by one.
DECRBY       DB.Str().Incr          Decrements a number from the integer value of a key.
GET          DB.Str().Get           Returns the value of a key.
GETSET       DB.Str().SetWith       Sets the key to a new value and returns the prev value.
INCR         DB.Str().Incr          Increments the integer value of a key by one.
INCRBY       DB.Str().Incr          Increments the integer value of a key by a number.
INCRBYFLOAT  DB.Str().IncrFloat     Increments the float value of a key by a number.
MGET         DB.Str().GetMany       Returns the values of one or more keys.
MSET         DB.Str().SetMany       Sets the values of one or more keys.
PSETEX       DB.Str().SetExpires    Sets the value and expiration time (in ms) of a key.
SET          DB.Str().Set           Sets the value of a key.
SETEX        DB.Str().SetExpires    Sets the value and expiration (in sec) time of a key.
SETNX        DB.Str().SetWith       Sets the value of a key when the key doesn't exist.
```

The following string-related commands are not planned for 1.0:

```
APPEND  GETDEL  GETEX  GETRANGE  LCS  MSETNX  SETRANGE  STRLEN  SUBSTR
```

### Lists

Lists are sequences of strings sorted by insertion order. Redka supports the following list-related commands:

```
Command      Go API                      Description
-------      ------                      -----------
LINDEX       DB.List().Get               Returns an element by its index.
LINSERT      DB.List().Insert*           Inserts an element before or after another element.
LLEN         DB.List().Len               Returns the length of a list.
LPOP         DB.List().PopFront          Returns the first element after removing it.
LPUSH        DB.List().PushFront         Prepends an element to a list.
LRANGE       DB.List().Range             Returns a range of elements.
LREM         DB.List().Delete*           Removes elements from a list.
LSET         DB.List().Set               Sets the value of an element by its index.
LTRIM        DB.List().Trim              Removes elements from both ends a list.
RPOP         DB.List().PopBack           Returns the last element after removing it.
RPOPLPUSH    DB.List().PopBackPushFront  Removes the last element and pushes it to another list.
RPUSH        DB.List().PushBack          Appends an element to a list.
```

The following list-related commands are not planned for 1.0:

```
BLMOVE  BLMPOP  BLPOP  BRPOP  BRPOPLPUSH  LMOVE  LMPOP
LPOS  LPUSHX  RPUSHX
```

### Sets

Sets are unordered collections of unique strings. Redka supports the following set-related commands:

```
Command      Go API                 Description
-------      ------                 -----------
SADD         DB.Set().Add           Adds one or more members to a set.
SCARD        DB.Set().Len           Returns the number of members in a set.
SDIFF        DB.Set().Diff          Returns the difference of multiple sets.
SDIFFSTORE   DB.Set().DiffStore     Stores the difference of multiple sets.
SINTER       DB.Set().Inter         Returns the intersection of multiple sets.
SINTERSTORE  DB.Set().InterStore    Stores the intersection of multiple sets.
SISMEMBER    DB.Set().Exists        Determines whether a member belongs to a set.
SMEMBERS     DB.Set().Items         Returns all members of a set.
SMOVE        DB.Set().Move          Moves a member from one set to another.
SPOP         DB.Set().Pop           Returns a random member after removing it.
SRANDMEMBER  DB.Set().Random        Returns a random member from a set.
SREM         DB.Set().Delete        Removes one or more members from a set.
SSCAN        DB.Set().Scanner       Iterates over members of a set.
SUNION       DB.Set().Union         Returns the union of multiple sets.
SUNIONSTORE  DB.Set().UnionStore    Stores the union of multiple sets.
```

The following set-related commands are not planned for 1.0:

```
SINTERCARD  SMISMEMBER
```

### Hashes

Hashes are field-value (hash)maps. Redka supports the following hash-related commands:

```
Command       Go API                  Description
-------       ------------------      -----------
HDEL          DB.Hash().Delete        Deletes one or more fields and their values.
HEXISTS       DB.Hash().Exists        Determines whether a field exists.
HGET          DB.Hash().Get           Returns the value of a field.
HGETALL       DB.Hash().Items         Returns all fields and values.
HINCRBY       DB.Hash().Incr          Increments the integer value of a field.
HINCRBYFLOAT  DB.Hash().IncrFloat     Increments the float value of a field.
HKEYS         DB.Hash().Keys          Returns all fields.
HLEN          DB.Hash().Len           Returns the number of fields.
HMGET         DB.Hash().GetMany       Returns the values of multiple fields.
HMSET         DB.Hash().SetMany       Sets the values of multiple fields.
HSCAN         DB.Hash().Scanner       Iterates over fields and values.
HSET          DB.Hash().SetMany       Sets the values of one or more fields.
HSETNX        DB.Hash().SetNotExists  Sets the value of a field when it doesn't exist.
HVALS         DB.Hash().Exists        Returns all values.
```

The following hash-related commands are not planned for 1.0:

```
HRANDFIELD  HSTRLEN
```

### Sorted sets

Sorted sets (zsets) are collections of unique strings ordered by each string's associated score. Redka supports the following sorted set related commands:

```
Command           Go API                  Description
-------           ------                  -----------
ZADD              DB.ZSet().AddMany       Adds or updates one or more members of a set.
ZCARD             DB.ZSet().Len           Returns the number of members in a set.
ZCOUNT            DB.ZSet().Count         Returns the number of members of a set within a range of scores.
ZINCRBY           DB.ZSet().Incr          Increments the score of a member in a set.
ZINTER            DB.ZSet().InterWith     Returns the intersection of multiple sets.
ZINTERSTORE       DB.ZSet().InterWith     Stores the intersection of multiple sets in a key.
ZRANGE            DB.ZSet().RangeWith     Returns members of a set within a range of indexes.
ZRANGEBYSCORE     DB.ZSet().RangeWith     Returns members of a set within a range of scores.
ZRANK             DB.ZSet().GetRank       Returns the index of a member in a set ordered by ascending scores.
ZREM              DB.ZSet().Delete        Removes one or more members from a set.
ZREMRANGEBYRANK   DB.ZSet().DeleteWith    Removes members of a set within a range of indexes.
ZREMRANGEBYSCORE  DB.ZSet().DeleteWith    Removes members of a set within a range of scores.
ZREVRANGE         DB.ZSet().RangeWith     Returns members of a set within a range of indexes in reverse order.
ZREVRANGEBYSCORE  DB.ZSet().RangeWith     Returns members of a set within a range of scores in reverse order.
ZREVRANK          DB.ZSet().GetRankRev    Returns the index of a member in a set ordered by descending scores.
ZSCAN             DB.ZSet().Scan          Iterates over members and scores of a set.
ZSCORE            DB.ZSet().GetScore      Returns the score of a member in a set.
ZUNION            DB.ZSet().UnionWith     Returns the union of multiple sets.
ZUNIONSTORE       DB.ZSet().UnionWith     Stores the union of multiple sets in a key.
```

The following sorted set related commands are not planned for 1.0:

```
BZMPOP  BZPOPMAX  BZPOPMIN  ZDIFF  ZDIFFSTORE  ZINTERCARD
ZLEXCOUNT  ZMPOP  ZMSCORE  ZPOPMAX  ZPOPMIN  ZRANDMEMBER
ZRANGEBYLEX  ZRANGESTORE  ZREMRANGEBYLEX  ZREVRANGEBYLEX
```

### Key management

Redka supports the following key management (generic) commands:

```
Command    Go API                    Description
-------    ------                    -----------
DBSIZE     DB.Key().Len              Returns the total number of keys.
DEL        DB.Key().Delete           Deletes one or more keys.
EXISTS     DB.Key().Count            Determines whether one or more keys exist.
EXPIRE     DB.Key().Expire           Sets the expiration time of a key (in seconds).
EXPIREAT   DB.Key().ExpireAt         Sets the expiration time of a key to a Unix timestamp.
FLUSHDB    DB.Key().DeleteAll        Deletes all keys from the database.
KEYS       DB.Key().Keys             Returns all key names that match a pattern.
PERSIST    DB.Key().Persist          Removes the expiration time of a key.
PEXPIRE    DB.Key().Expire           Sets the expiration time of a key in ms.
PEXPIREAT  DB.Key().ExpireAt         Sets the expiration time of a key to a Unix ms timestamp.
RANDOMKEY  DB.Key().Random           Returns a random key name from the database.
RENAME     DB.Key().Rename           Renames a key and overwrites the destination.
RENAMENX   DB.Key().RenameNotExists  Renames a key only when the target key name doesn't exist.
SCAN       DB.Key().Scanner          Iterates over the key names in the database.
TTL        DB.Key().Get              Returns the expiration time in seconds of a key.
TYPE       DB.Key().Get              Returns the type of value stored at a key.
```

The following generic commands are not planned for 1.0:

```
COPY  DUMP  EXPIRETIME  MIGRATE  MOVE  OBJECT  PEXPIRETIME
PTTL  RESTORE  SORT  SORT_RO  TOUCH  TTL  TYPE  UNLINK
WAIT  WAITAOF
```

### Transactions

Redka supports the following transaction commands:

```
Command    Go API                 Description
-------    ------                 -----------
DISCARD    DB.View / DB.Update    Discards a transaction.
EXEC       DB.View / DB.Update    Executes all commands in a transaction.
MULTI      DB.View / DB.Update    Starts a transaction.
```

Unlike Redis, Redka's transactions are fully ACID, providing automatic rollback in case of failure.

The following transaction commands are not planned for 1.0:

```
UNWATCH  WATCH
```

### Server/connection management

Redka supports only a couple of server and connection management commands:

```
Command    Go API                Description
-------    ------                -----------
ECHO       -                     Returns the given string.
PING       -                     Returns the server's liveliness response.
```

The rest of the server and connection management commands are not planned for 1.0.

## Installation

Redka can be installed as a standalone Redis-compatible server, or as a Go module for in-process use.

## Standalone server

Redka server is a single-file binary. Download it from the [releases](https://github.com/nalgeon/redka/releases).

Linux (x86 CPU only):

```shell
curl -L -O "https://github.com/nalgeon/redka/releases/download/v0.4.0/redka_linux_amd64.zip"
unzip redka_linux_amd64.zip
chmod +x redka
```

macOS (both x86 and ARM/Apple Silicon CPU):

```shell
curl -L -O "https://github.com/nalgeon/redka/releases/download/v0.4.0/redka_darwin_amd64.zip"
unzip redka_darwin_amd64.zip
# remove the build from quarantine
# (macOS disables unsigned binaries)
xattr -d com.apple.quarantine redka
chmod +x redka
```

Or pull with Docker as follows (x86/ARM):

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
./redka -h 0.0.0.0 -p 6379 data.db
```

Server defaults are host `localhost`, port `6379` and empty DB path.

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
    // Open or create the data.db file.
    db, err := redka.Open("data.db", nil)
    if err != nil {
        log.Fatal(err)
    }
    // Always close the database when you are finished.
    defer db.Close()
    // ...
}
```

Don't forget to import the driver (here I use `github.com/mattn/go-sqlite3`). Using `modernc.org/sqlite` is slightly different, see [example/modernc/main.go](example/modernc/main.go) for details.

To open an in-memory database that doesn't persist to disk, use the following path:

```go
// All data is lost when the database is closed.
redka.Open("file:redka?mode=memory&cache=shared")
```

After opening the database, call `redka.DB` methods to run individual commands:

```go
db.Str().Set("name", "alice")
db.Str().Set("age", 25)

count, err := db.Key().Count("name", "age", "city")
slog.Info("count", "count", count, "err", err)

name, err := db.Str().Get("name")
slog.Info("get", "name", name, "err", err)
```

```
count count=2 err=<nil>
get name="alice" err=<nil>
```

See the full example in [example/simple/main.go](example/simple/main.go).

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

See the full example in [example/tx/main.go](example/tx/main.go).

See the [package documentation](https://pkg.go.dev/github.com/nalgeon/redka) for API reference.

## Persistence

Redka stores data in a SQLite database using the following tables:

```
rkey
---
id       integer primary key
key      text not null
type     integer not null    -- 1 string, 2 list, 3 set, 4 hash, 5 sorted set
version  integer not null    -- incremented when the key value is updated
etime    integer             -- expiration timestamp in unix milliseconds
mtime    integer not null    -- modification timestamp in unix milliseconds
len      integer             -- number of child elements

rstring
---
kid      integer not null    -- FK -> rkey.id
value    blob not null

rlist
---
kid      integer not null    -- FK -> rkey.id
pos      real not null       -- is used for ordering, but is not an index
elem     blob not null

rset
---
kid      integer not null    -- FK -> rkey.id
elem     blob not null

rhash
---
kid      integer not null    -- FK -> rkey.id
field    text not null
value    blob not null

rzset
---
kid      integer not null    -- FK -> rkey.id
elem     blob not null
score    real not null
```

To access the data with SQL, use views instead of tables:

```sql
select * from vstring;
```

```
┌─────┬──────┬───────┬───────┬─────────────────────┐
│ kid │ key  │ value │ etime │        mtime        │
├─────┼──────┼───────┼───────┼─────────────────────┤
│ 1   │ name │ alice │       │ 2024-04-03 16:58:14 │
│ 2   │ age  │ 50    │       │ 2024-04-03 16:34:52 │
└─────┴──────┴───────┴───────┴─────────────────────┘
```

`etime` and `mtime` are in UTC.

There is a separate view for every data type:

```
vkey  vstring  vlist  vset  vhash  vzset
```

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

Redis:

```
redis-server --appendonly no
redis-benchmark -p 6379 -q -c 10 -n 1000000 -r 10000 -t get,set

SET: 133262.25 requests per second, p50=0.055 msec
GET: 139217.59 requests per second, p50=0.055 msec
```

Redka (in-memory):

```
./redka -p 6380
redis-benchmark -p 6380 -q -c 10 -n 1000000 -r 10000 -t get,set

SET: 34927.18 requests per second, p50=0.175 msec
GET: 52173.01 requests per second, p50=0.143 msec
```

Redka (persisted to disk):

```
./redka -p 6380 data.db
redis-benchmark -p 6380 -q -c 10 -n 1000000 -r 10000 -t get,set

SET: 26028.11 requests per second, p50=0.215 msec
GET: 93923.17 requests per second, p50=0.071 msec
```

So while Redka is 2-5 times slower than Redis (not surprising, since we are comparing a relational database to a key-value data store), it can still do 26K writes/sec and 94K reads/sec, which is pretty good if you ask me.

Note that running in a container may result in poorer performance.

## Roadmap

The project is functionally ready for 1.0. Feel free to try it in non-critical production scenarios and provide feedback in the issues.

The 1.0 release will include the following features:

-   ✅ Strings.
-   ✅ Lists.
-   ✅ Sets.
-   ✅ Hashes.
-   ✅ Sorted sets.
-   ✅ Key management.
-   ✅ Transactions.

✅ = done, ⏳ = in progress, ⬜ = next in line

Future versions may include additional data types (such as streams, HyperLogLog or geo), features like publish/subscribe, and more commands for existing types.

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

Logo font by [Ek Type](https://ektype.in/).

### License

Copyright 2024 [Anton Zhiyanov](https://antonz.org/).

The software is available under the BSD-3-Clause license.

### Stay tuned

★ [Subscribe](https://antonz.org/subscribe/) to stay on top of new features.

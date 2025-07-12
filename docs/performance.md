# Performance

I've compared Redka with Redis using [redis-benchmark](https://redis.io/docs/management/optimization/benchmarks/) with the following parameters:

-   10 parallel connections
-   1000000 requests
-   10000 randomized keys
-   GET/SET commands

## Results

Redis:

```text
redis-server --appendonly no
redis-benchmark -p 6379 -q -c 10 -n 1000000 -r 10000 -t get,set

SET: 133262.25 requests per second, p50=0.055 msec
GET: 139217.59 requests per second, p50=0.055 msec
```

Redka (SQLite, in-memory):

```text
./redka -p 6380
redis-benchmark -p 6380 -q -c 10 -n 1000000 -r 10000 -t get,set

SET: 36188.62  requests per second, p50=0.167 msec
GET: 104405.93 requests per second, p50=0.063 msec
```

Redka (SQLite, persisted to disk):

```text
./redka -p 6380 redka.db
redis-benchmark -p 6380 -q -c 10 -n 1000000 -r 10000 -t get,set

SET: 26773.76  requests per second, p50=0.215 msec
GET: 103092.78 requests per second, p50=0.063 msec
```

Redka (PostgreSQL):

```text
./redka -p 6380 "postgres://redka:redka@localhost:5432/redka?sslmode=disable"
redis-benchmark -p 6380 -q -c 10 -n 100000 -r 10000 -t get,set

SET: 11941.72 requests per second, p50=0.775 msec
GET: 25766.55 requests per second, p50=0.359 msec
```

So while Redka is noticeably slower than Redis (not surprising, since we are comparing a relational database to a key-value data store), it can still handle tens of thousands of operations per second. That should be more than enough for many apps.

## Environment

Hardware: Apple M1 8-core CPU, 16GB RAM

SQLite settings:

```text
pragma journal_mode = wal;
pragma synchronous = normal;
pragma temp_store = memory;
pragma mmap_size = 268435456;
pragma foreign_keys = on;
```

PostgreSQL settings:

```text
checkpoint_completion_target=0.9
effective_cache_size=4GB
maintenance_work_mem=512MB
max_wal_size=1GB
random_page_cost=1.1
shared_buffers=1GB
wal_buffers=16MB
```

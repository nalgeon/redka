# Performance

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

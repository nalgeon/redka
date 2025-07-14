<img alt="Redka" src="logo.svg" height="80" align="center">

Redka aims to reimplement the core parts of Redis with SQL, while remaining compatible with Redis API.

Highlights:

-   Data doesn't have to fit in RAM.
-   Supports ACID transactions.
-   SQL views for easier analysis and reporting.
-   Uses SQLite or PostgreSQL as a backend.
-   Runs in-process (Go API) or as a standalone server.
-   Implements Redis commands and wire protocol (RESP).

Redka is [functionally ready](docs/roadmap.md) for 1.0. Feel free to try it in non-critical production scenarios and provide feedback in the issues.

## Use cases

Here are some situations where Redka might be helpful:

_Embedded cache for Go applications_. If your Go app already uses SQLite or just needs a built-in key-value store, Redka is a natural fit. It gives you Redis-like features without the hassle of running a separate server. You're not limited to just get/set with expiration, of course — more advanced structures like lists, maps, and sets are also available.

_Lightweight testing environment_. Your app uses Redis in production, but setting up a Redis server for local development or integration tests can be a hassle. Redka with an in-memory database offers a fast alternative to test containers, providing full isolation for each test run.

_Postgres-first data structures_. If you prefer to use PostgreSQL for everything but need Redis-like data structures, Redka can use your existing database as the backend. This way, you can manage both relational data and specialized data structures with the same tools and transactional guarantees.

## Commands

Redka supports five core Redis data types:

-   [Strings](docs/commands/strings.md) are the most basic Redis type, representing a sequence of bytes.
-   [Lists](docs/commands/lists.md) are sequences of strings sorted by insertion order.
-   [Sets](docs/commands/sets.md) are unordered collections of unique strings.
-   [Hashes](docs/commands/hashes.md) are field-value (hash)maps.
-   [Sorted sets](docs/commands/sorted-sets.md) (zsets) are collections of unique strings ordered by each string's associated score.

Redka also provides commands for [key management](docs/commands/keys.md), [server/connection management](docs/commands/server.md), and [transactions](docs/commands/transactions.md).

## Installation and usage

Redka comes in two flavors:

-   Standalone Redis-compatible server: [installation](docs/install-standalone.md), [usage](docs/usage-standalone.md).
-   Go module for in-process use: [installation](docs/install-module.md), [usage](docs/usage-module.md).

You can also run an [in-process Redka server](example/server/main.go) as a lightweight alternative to Redis test containers, or as a small-scale production instance.

## Storage

Redka can use either SQLite or PostgreSQL as its backend. It stores data in a [relational database](docs/persistence.md) with a simple schema and provides views for better introspection.

## Performance

Redka is not about raw performance. You can't beat a specialized data store like Redis with a general-purpose relational backend like SQLite. However, Redka can still handle tens of thousands of operations per second, which should be more than enough for many apps.

See the [benchmarks](docs/performance.md) for more details.

## Contributing

Contributions are welcome. For anything other than bugfixes, please first open an issue to discuss what you want to change.

Make sure to add or update tests as needed.

## Acknowledgements

Redka would not be possible without these great projects and their creators:

-   [Redis](https://redis.io/) ([Salvatore Sanfilippo](https://github.com/antirez)). It's such an amazing idea to go beyond the get-set paradigm and provide a convenient API for more complex data structures.
-   [SQLite](https://sqlite.org/) ([D. Richard Hipp](https://www.sqlite.org/crew.html)). The in-process database powering the world.
-   [Redcon](https://github.com/tidwall/redcon) ([Josh Baker](https://github.com/tidwall)). A very clean and convenient implementation of a RESP server.

Logo font by [Ek Type](https://ektype.in/).

## Support

Redka is mostly a [one-man](https://antonz.org/) project, not backed by a VC fund or anything.

If you find Redka useful, please star it on GitHub and spread the word among your peers. It really helps to move the project forward.

If you use Redka for commercial purposes, consider [purchasing support](https://antonz.gumroad.com/l/redka-plus).

★ [Subscribe](https://antonz.org/subscribe/) to stay on top of new features.

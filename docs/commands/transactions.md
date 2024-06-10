# Transactions

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

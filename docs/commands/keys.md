# Key management

Redka supports the following key management (generic) commands:

```
Command    Go API                    Description
-------    ------                    -----------
DBSIZE     DB.Key().Len              Returns the total number of keys.
DEL        DB.Key().Delete           Deletes one or more keys.
EXISTS     DB.Key().Count            Determines whether one or more keys exist.
EXPIRE     DB.Key().Expire           Sets the expiration time of a key (in seconds).
EXPIREAT   DB.Key().ExpireAt         Sets the expiration time of a key to a Unix timestamp.
FLUSHALL   DB.Key().DeleteAll        Deletes all keys from the database.
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

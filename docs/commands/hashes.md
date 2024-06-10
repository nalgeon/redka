# Hashes

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

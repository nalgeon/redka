# Strings

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
STRLEN       DB.Str().Get           Returns the length of a value in bytes.
```

The following string-related commands are not planned for 1.0:

```
APPEND  GETDEL  GETEX  GETRANGE  LCS  MSETNX  SETRANGE  SUBSTR
```

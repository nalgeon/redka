# Sets

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

# Sorted sets

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

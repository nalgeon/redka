# Lists

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

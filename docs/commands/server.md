# Server/connection management

Redka supports only a couple of server and connection management commands:

```
Command    Go API                Description
-------    ------                -----------
ECHO       -                     Returns the given string.
LOLWUT     -                     Provides an answer to a yes/no question.
PING       -                     Returns the server's liveliness response.
SELECT     -                     Changes the selected database (no-op).
```

The rest of the server and connection management commands are not planned for 1.0.

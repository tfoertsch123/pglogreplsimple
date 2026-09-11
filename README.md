# pglogreplsimple
A wrapper around `github.com/jackc/pglogrepl`. It represents the decoded
WAL stream as an iterator and handles feedback to the server and (re)connection
behind the scenes.

A more elaborate example can be found in the `example` directory.

## Installation

```bash
go get -u github.com/tfoertsch123/pglogreplsimple
```

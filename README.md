# GoRedis

A lightweight Redis implementation written in Go, serves as presonal learning project, tested using [codecrafters](https://app.codecrafters.io/courses/redis).

## Table of Contents
- [Features](#features)
- [Installation](#installation)
- [Command Compatibility](#command-compatibility)
- [Contributing](#ontributing)

## Features

- Pure Go implementation
- Сompatible with redis-server/redis-cli
- Redis protocol support
- Key-value operations
- Streams support
- Transactions support
- Replication capabilities
- RDB persistence support


## Instalation

clone this repository
```bash
git clone https://github.com/4c656f/redis-go
```
install golang >= 1.22

start server

```bash
./your_program.sh
```
to execute commands via redis-cli run redis-server on custom port
```bash
redis-server --port 3780
```
execute command
```bash
redis-cli ping
```

## Command Compatibility

| Category         | Command    | Arguments                                                                                                   |
| ---------------- | ---------- | ----------------------------------------------------------------------------------------------------------- |
| **Connection**   | PING       | [message]                                                                                                   |
|                  | PONG       | [message]                                                                                                   |
|                  | ECHO       | message                                                                                                     |
| **Key-Value**    | SET        | key value [EX seconds] [PX milliseconds] [NX\|XX]                                                           |
|                  | GET        | key                                                                                                         |
|                  | KEYS       | pattern                                                                                                     |
|                  | TYPE       | key                                                                                                         |
|                  | INCR       | key                                                                                                         |
| **Server**       | INFO       | [all/replication]                                                                                           |
|                  | CONFIG     | GET dir/dbfilename                                                                                          |
| **Replication**  | REPLCONF   | listening-port / GETACK ackType / ACK offset / capa psynch2                                                 |
|                  | PSYNC      | replicationid offset                                                                                        |
|                  | FULLRESYNC | replicationid offset                                                                                        |
|                  | WAIT       | numreplicas timeout                                                                                         |
| **Streams**      | XADD       | key [NOMKSTREAM] [<MAXLEN / MINID> [= / ~] threshold [LIMIT count]] <\* / id> field value [field value ...] |
|                  | XRANGE     | key start end [COUNT count]                                                                                 |
|                  | XREAD      | [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] ID [ID ...]                                        |
| **Transactions** | MULTI      | (no arguments)                                                                                              |
|                  | EXEC       | (no arguments)                                                                                              |
|                  | DISCARD    | (no arguments)                                                                                              |
| **Generic**      | OK         | (no arguments)                                                                                              |
|                  | ERROR      | (no arguments)                                                                                              |

## Contributing

I will be glad to receive any of your questions/suggestions/contributions to this project! 
contact me via:

[Twitter](https://x.com/4c656f)

[Email](mailto:tarabrinleonid@gmail.com)

[Telegram](https://t.me/c656f)


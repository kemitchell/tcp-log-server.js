Serve an append-only log over TCP.

- Uses a dead-simple, JSON-based protocol.
- Stores entries as content-addressed blobs.
- Persists to disk by default.

```bash
npm install --global tcp-log-server
tcp-log-server
```

## Logs

The server uses [pino] logging.  To improve console log output:

```bash
npm install --global pino
tcp-log-server | tee server.log | pino
```

[pino]: https://npmjs.com/packages/pino

## Environment

Configure `tcp-log-server` with environment variables:

- `PORT` for TCP
- `BLOBS` directory for entry JSON files, named by hash, or `memory` to
  store in memory
- `LEVELDOWN` directory for [LevelDB] data files, or `memory` to
  store in memory

[LevelDB]: https://npmjs.com/packages/leveldown

## Node.js

The package exports a factory function.  Given a [pino] log, a
[LevelUP] instance, an [abstract-blob-store], and an `EventEmitter`,
it returns a TCP connection handler function suitable for
`net.createServer(handler)`.  Any [LevelDOWN] and [abstract-blob-store]
will do.  Store log entries or blobs in memory, in a remote store,
or in whatever combination you like.

[LevelUP]: https://npmjs.com/packages/levelup

[abstract-blob-store]: https://npmjs.com/packages/abstract-blob-store

[LevelDOWN]: https://www.npmjs.com/package/abstract-leveldown

There is one caveat: The storage back-end for the LevelUP must support
[snapshots].  [level-party] and a few other helper packages prevent
snapshotting.

[snapshots]: https://github.com/level/leveldown#snapshots

[level-party]: https://www.npmjs.com/package/level-party

## Protocol

The server accepts TCP connections.  It enables keep-alive on each
socket.  All messages are [newline-delimited JSON][ndjson] objects.
[tcp-log-client] provides a high-level interface.

[tcp-log-client]: https://npmjs.com/packages/tcp-log-client

[ndjson]: https://npmjs.com/packages/ndjson

Clients can send:

### Read

```json
{"from":1,"read":5}
```

On receipt, the server will begin sending up to `read` log entries
with indices greater than or equal to `from`, in ascending-index order.
(The lowest possible index is `1`.)

Each entry will be sent like:

```json
{"index":"1","entry":{"some":"entry"}}
```

Servers may report errors reading specific log entries:

```json
{"index":45,"error":"some-error"}
```

If the server reaches the head of its log before sending `read`
entries, it will send:

```json
{"current":true}
```

Once the server has sent `read` entries, it will report the index of
the head of its log:

```json
{"head":100}
```

### Write

```json
{"id":"some-id","entry":{"arbitrary":"data"}}
```

`require('uuid').v4()`, with the [uuid] package, is an easy way to
generate id strings.

[uuid]: https://npmjs.com/packages/uuid

Once successfully appended to the log, the server will confirm the
index of the newly appended entry.

```json
{"id":"some-id","index":44}
```

If there is an error, the server will instead respond:

```json
{"id":"some-id-string","error":"error-string"}
```

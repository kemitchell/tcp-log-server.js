Serve an append-only log over TCP.

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
- `BLOBS` directory for entry JSON files, named by hash
- `LEVELDB` directory for [LevelDB] data files

[LevelDB]: https://npmjs.com/packages/leveldown

## Node.js

The package exports a factory function.  Given a [pino] log, a
[LevelUP] instance, an [abstract-blob-store], and an `EventEmitter`,
it returns a TCP connection handler function suitable for
`net.createServer(handler)`.

[LevelUP]: https://npmjs.com/packages/levelup

[abstract-blob-store]: https://npmjs.com/packages/abstract-blob-store

## Protocol

The server accepts TCP connections.  It enables keep-alive on each
socket.  All messages are [newline-delimited JSON][ndjson] objects.
[tcp-log-client] provides a high-level interface.

[tcp-log-client]: https://npmjs.com/packages/tcp-log-client

[ndjson]: https://npmjs.com/packages/ndjson

Clients can send:

### Read

```json
{"type":"read","from":0}
```

On receipt, the server will begin sending log entries with indices
greater than or equal to `from`.  Each entry will be sent like:

```json
{"index":"1","entry":{"some":"entry"}}
```

The server will send:

```json
{"current":true}
```

once it has sent all entries in the log as of the time the read
message was received.

### Write

```json
{"type":"write","id":"some-id","entry":{"arbitrary":"data"}}
```

`require('uuid').v4()`, with the [uuid] package, is an easy way to
generate id strings.

[uuid]: https://npmjs.com/packages/uuid

Once successfully appended to the log, the server will confirm the
index of the newly appended entry.

```json
{"id":"some-id","event":"wrote","index":44}
```

The server will _not_ echo the new entry back to the client that
writes it.

If there is an error, the server will instead respond:

```json
{"id":"some-id-string","error":"error-string"}
```

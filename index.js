var asyncQueue = require('async.queue')
var concatStream = require('concat-stream')
var duplexJSON = require('duplex-json-stream')
var endOfStream = require('end-of-stream')
var stringify = require('json-stable-stringify')
var through2 = require('through2')
var uuid = require('uuid').v4

module.exports = function factory (
  log, file, blobs, emitter, hashFunction
) {
  return function tcpConnectionHandler (connection) {
    // Connections will be held open long-term, and may sit idle.
    // Enable keep-alive to keep them from dropping.
    connection.setKeepAlive(true)

    // Set up a child log for this connection, identified by UUID.
    var connectionLog = log.child({ connection: uuid() })
    connectionLog.info({
      event: 'connected',
      address: connection.remoteAddress,
      port: connection.removePort
    })

    // Log end-of-connection events.
    connection
      .once('end', function () {
        connectionLog.info({ event: 'end' })
      })
      .once('error', /* istanbul ignore next */ function (error) {
        connectionLog.error(error)
      })
      .once('close', function (error) {
        connectionLog.info({
          event: 'close',
          error: error
        })
      })

    // Send newline-delimited JSON back and forth across the connection.
    var json = duplexJSON(connection)
      .once('error', function () {
        disconnect('invalid JSON')
      })

    // Whether the client is currently reading the log. When reading, an
    // object recording information about the state of the read.
    //
    // - doneStreaming (Boolean): Whether the server is done streaming
    //   old entries from LevelUP.
    //
    // - buffer (Array): Contains entries made after the server
    //   started streaming old entries, but before it finished.
    //
    // - streams ([Stream]): Array of streams used to read data.
    //
    // - from (Number): The index of the first entry to send.
    //
    // - through (Number): The index of the last entry to send.
    var reading = false

    // An asynchronous queue for appending to the log. Ensures that each
    // append operation can read the head of the log and number itself
    // appropriately.
    var writeQueue = asyncQueue(function write (message, done) {
      // Compute the hash of the entry's content.
      var content = stringify(message.entry)
      var hash = hashFunction(content)
      // Create a child log for this entry write.
      var writeLog = connectionLog.child({ hash: hash })
      // Append the entry payload in the blob store, by hash.
      blobs.createWriteStream({ key: hashToPath(hash) })
        .once('error', /* istanbul ignore next */ function (error) {
          writeLog.error(error)
          json.write({
            id: message.id,
            error: error.toString()
          }, done)
        })
        .once('finish', function appendToLog () {
          // Append an entry to the log with the hash of the entry.
          writeLog.info({
            event: 'appending',
            hash: hash
          })
          file.append(hash, function (error, index) {
            /* istanbul ignore if */
            if (error) {
              writeLog.error(error)
              json.write({
                id: message.id,
                error: error.toString()
              }, done)
            } else {
              writeLog.info({ event: 'wrote' })
              // Send confirmation.
              json.write({
                id: message.id,
                index: index
              }, done)
              // Emit an event.
              emitter.emit('entry', index, message.entry, connection)
            }
          })
        })
        .end(content)
    })

    // Route client messages to appropriate handlers.
    json.on('data', function routeMessage (message) {
      connectionLog.info({ event: 'message', message: message })
      if (isReadMessage(message)) {
        if (reading) {
          connectionLog.warn('already reading')
          json.write({ error: 'already reading' })
        } else {
          onReadMessage(message)
        }
      } else if (isWriteMessage(message)) {
        writeQueue.push(message)
      } else {
        connectionLog.warn({
          event: 'invalid',
          message: message
        })
        json.write({ error: 'invalid message' })
      }
    })

    // Handle read requests.
    function onReadMessage (message) {
      // A read advances, in order, through three phases:
      //
      // Phase 1. Stream entries from a snapshot of the LevelUP store
      //          created by `.createStream`, fetching their content
      //          from the blob store.  Buffer entries appended while
      //          streaming.
      //
      // Phase 2. Send entries that were appended while completing
      //          Phase 1 from the buffer.
      //
      // Phase 3. Forward entries as they are appended.
      //
      // Comments with "Phase 1", "Phase 2", and "Phase 3" appear in
      // relevant places below.
      reading = {
        doneStreaming: false,
        buffer: [],
        streams: [],
        from: message.from,
        through: message.from + message.read - 1
      }

      // Start buffering new entries for Phase 2.
      setImmediate(function () {
        emitter.addListener('entry', onAppend)
      })

      // Phase 1: Stream entries from the LevelUP store.
      var streamLog = connectionLog.child({ phase: 'stream' })
      streamLog.info({ event: 'create' })

      var readStream = file.createStream({
        from: message.from - 1,
        limit: message.read
      })

      // Track the highest index seen, so we know when we have sent
      // all the entries requested.
      var highestIndex = 0

      // For each index-hash pair, read the corresponding content from
      // the blog store and forward a complete entry object.
      var transform = through2.obj(function (record, _, done) {
        highestIndex = record.index
        blobs.createReadStream({ key: hashToPath(record.entry) })
          .once('error', onFatalError)
          .pipe(concatStream(function (json) {
            done(null, {
              index: record.index,
              entry: JSON.parse(json)
            })
          }))
      })

      // Push references so the streams so they can be unpiped and
      // destroyed later.
      reading.streams.push(readStream)
      reading.streams.push(transform)

      // Build the data pipeline.
      readStream
        .once('error', onFatalError)
        .pipe(transform)
        .once('error', onFatalError)
        .pipe(json, { end: false })

      endOfStream(transform, function (error) {
        /* istanbul ignore if */
        if (error) {
          onFatalError(error)
        } else {
          // Phase 2: Send buffered entries.
          reading.buffer.forEach(function (buffered) {
            highestIndex = buffered.index
            streamLog.info({
              event: 'unbuffer',
              index: buffered.index
            })
            sendEntry(buffered.index, buffered.entry)
          })
          if (sentAllRequested()) {
            finish()
          } else {
            json.write({ current: true })
            // Set flags to start Phase 3.
            reading.doneStreaming = true
            reading.buffer = null
          }
        }
      })

      function onAppend (index, entry, fromConnection) {
        // Do not send entries from earlier in the log than requested.
        if (index < reading.from) {
          // pass
        // Do not send entries later than requested.
        } else if (index > reading.through) {
          // pass
        // Phase 3: Forward entries as they are appended.
        } else if (reading.doneStreaming) {
          connectionLog.info({
            event: 'forward',
            index: index
          })
          sendEntry(index, entry)
          highestIndex = index
          if (sentAllRequested()) {
            finish()
          }
        // Buffer for Phase 2.
        } else {
          connectionLog.info({
            event: 'buffer',
            index: index
          })
          reading.buffer.push({
            index: index,
            entry: entry
          })
        }
      }

      function sentAllRequested () {
        return highestIndex === reading.through
      }

      function finish () {
        destroyStreams()
        emitter.removeListener('entry', onAppend)
        file.head(function (error, head) {
          /* istanbul ignore if */
          if (error) {
            streamLog.error(error)
            disconnect(error.toString())
          } else {
            json.write({ head: head })
            reading = false
          }
        })
      }

      function sendEntry (index, entry) {
        json.write({
          index: index,
          entry: entry
        })
        connectionLog.info({
          event: 'sent',
          index: index
        })
      }

      /* istanbul ignore next */
      function onFatalError (error) {
        streamLog.error(error)
        emitter.removeListener('entry', onAppend)
        disconnect(error.toString())
      }
    }

    function destroyStreams () {
      reading.streams.forEach(function (stream) {
        stream.unpipe()
        stream.destroy()
      })
    }

    function disconnect (error) {
      connectionLog.error({ error: error })
      json.write({ error: error })
      if (reading) {
        destroyStreams()
        writeQueue.kill()
      }
      reading = false
      connection.destroy()
    }
  }
}

function isReadMessage (argument) {
  return (
    typeof argument === 'object' &&
    has(argument, 'from', isPositiveInteger) &&
    has(argument, 'read', isPositiveInteger)
  )
}

function isWriteMessage (argument) {
  return (
    typeof argument === 'object' &&
    has(argument, 'id', function (id) {
      return id.length > 0
    }) &&
    has(argument, 'entry', function (entry) {
      return typeof entry === 'object'
    })
  )
}

function has (argument, key, predicate) {
  return (
    Object.prototype.hasOwnProperty.call(argument, key) &&
    typeof predicate === 'function'
      ? predicate(argument[key])
      : argument[key] === predicate
  )
}

function isPositiveInteger (argument) {
  return Number.isInteger(argument) && argument > 0
}

function hashToPath (hash) {
  return hash.slice(0, 2) + '/' + hash.slice(2)
}

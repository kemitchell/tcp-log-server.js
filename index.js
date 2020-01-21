const crypto = require('crypto')
const Lock = require('lock').Lock
const asyncQueue = require('async.queue')
const concatStream = require('concat-stream')
const duplexJSON = require('duplex-json-stream')
const endOfStream = require('end-of-stream')
const fs = require('fs')
const split2 = require('split2')
const stringify = require('fast-json-stable-stringify')
const through2 = require('through2')
const uuid = require('uuid').v4

const lock = Lock()

module.exports = (options) => {
  const { log, file, blobs, emitter } = options
  const hashFunction = options.hashFunction || sha256
  const hashBytes = options.hashBytes || 64
  const lineBytes = hashBytes + 1
  return (connection) => {
    // Connections will be held open long-term, and may sit idle.
    // Enable keep-alive to keep them from dropping.
    connection.setKeepAlive(true)

    // Set up a child log for this connection, identified by UUID.
    const connectionLog = log.child({ connection: uuid() })
    connectionLog.info({
      address: connection.remoteAddress,
      port: connection.removePort
    }, 'connected')

    // Log end-of-connection events.
    connection
      .once('end', () => {
        connectionLog.info('end')
      })
      .once('error', /* istanbul ignore next */ (error) => {
        connectionLog.error(error)
      })
      .once('close', (error) => {
        connectionLog.info({ error }, 'close')
      })

    // Send newline-delimited JSON back and forth across the connection.
    const json = duplexJSON(connection)
      .once('error', () => {
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
    let reading = false

    // An asynchronous queue for appending to the log. Ensures that each
    // append operation can read the head of the log and number itself
    // appropriately.
    const writeQueue = asyncQueue((message, done) => {
      // Compute the hash of the entry's content.
      const content = stringify(message.entry)
      const hash = hashFunction(content)
      // Create a child log for this entry write.
      const writeLog = connectionLog.child({ hash })
      // Append the entry payload in the blob store, by hash.
      blobs.createWriteStream(hash)
        .once('error', /* istanbul ignore next */ (error) => {
          writeLog.error(error)
          json.write({
            id: message.id,
            error: error.toString()
          }, done)
        })
        .once('finish', () => {
          lock(file, (unlock) => {
            done = unlock(done)
            // Append an entry to the log with the hash of the entry.
            writeLog.info({ hash }, 'appending')
            fs.writeFile(file, hash + '\n', { flag: 'a' }, (error) => {
              /* istanbul ignore if */
              if (error) {
                writeLog.error(error)
                return json.write({
                  id: message.id,
                  error: error.toString()
                }, done)
              }
              writeLog.info('wrote')
              readHead((error, index) => {
                if (error) return done(error)
                // Send confirmation.
                json.write({ id: message.id, index }, done)
                // Emit an event.
                emitter.emit('entry', index, message.entry, connection)
              })
            })
          })
        })
        .end(content)
    })

    // Route client messages to appropriate handlers.
    json.on('data', (message) => {
      connectionLog.info({ message }, 'message')
      if (isReadMessage(message)) {
        if (reading) {
          connectionLog.warn('already reading')
          return json.write({ error: 'already reading' })
        }
        return onReadMessage(message)
      } else if (isWriteMessage(message)) {
        return writeQueue.push(message)
      }
      connectionLog.warn({ message }, 'invalid')
      json.write({ error: 'invalid message' })
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
      setImmediate(() => {
        emitter.addListener('entry', onAppend)
      })

      // Phase 1: Stream entries from the LevelUP store.
      const streamLog = connectionLog.child({ phase: 'stream' })
      streamLog.info('create')

      const start = (message.from - 1) * lineBytes
      const end = start + (message.read * lineBytes) - 1
      const readStream = fs.createReadStream(file, { start, end })
      const split = split2('\n', { trailing: false })

      // Track the highest index seen, so we know when we have sent
      // all the entries requested.
      let highestIndex = message.from - 1

      // For each index-hash pair, read the corresponding content from
      // the blog store and forward a complete entry object.
      const transform = through2.obj((hash, _, done) => {
        highestIndex++
        blobs.createReadStream(hash)
          .once('error', onFatalError)
          .pipe(concatStream((json) => {
            done(null, {
              index: highestIndex,
              entry: JSON.parse(json)
            })
          }))
      })

      // Push references to the streams so they can be unpiped and
      // destroyed later.
      reading.streams.push(readStream)
      reading.streams.push(split)
      reading.streams.push(transform)

      // Build the data pipeline.
      readStream
        .once('error', onFatalError)
        .pipe(split)
        .once('error', onFatalError)
        .pipe(transform)
        .once('error', onFatalError)
        .pipe(json, { end: false })

      endOfStream(transform, (error) => {
        /* istanbul ignore if */
        if (error) return onFatalError(error)
        // Phase 2: Send buffered entries.
        reading.buffer.forEach((buffered) => {
          highestIndex = buffered.index
          streamLog.info({
            index: buffered.index
          }, 'unbuffered')
          sendEntry(buffered.index, buffered.entry)
        })
        if (sentAllRequested()) return finish()
        json.write({ current: true })
        // Set flags to start Phase 3.
        reading.doneStreaming = true
        reading.buffer = null
      })

      function onAppend (index, entry, fromConnection) {
        // Do not send entries from earlier in the log than requested.
        if (index < reading.from) return // pass
        // Do not send entries later than requested.
        if (index > reading.through) return // pass
        // Phase 3: Forward entries as they are appended.
        if (reading.doneStreaming) {
          connectionLog.info({ index }, 'forward')
          sendEntry(index, entry)
          highestIndex = index
          if (sentAllRequested()) finish()
          return
        }
        // Buffer for Phase 2.
        connectionLog.info({ index }, 'buffer')
        reading.buffer.push({ index, entry })
      }

      function sentAllRequested () {
        return highestIndex === reading.through
      }

      function finish () {
        destroyStreams()
        emitter.removeListener('entry', onAppend)
        readHead((error, head) => {
          /* istanbul ignore if */
          if (error) {
            streamLog.error(error)
            return disconnect(error.toString())
          }
          json.write({ head })
          reading = false
        })
      }

      function sendEntry (index, entry) {
        json.write({ index, entry })
        connectionLog.info({ index }, 'sent')
      }

      /* istanbul ignore next */
      function onFatalError (error) {
        streamLog.error(error)
        emitter.removeListener('entry', onAppend)
        disconnect(error.toString())
      }
    }

    function destroyStreams () {
      reading.streams.forEach((stream) => {
        stream.unpipe()
        stream.destroy()
      })
    }

    function disconnect (error) {
      connectionLog.error({ error })
      json.write({ error })
      if (reading) {
        destroyStreams()
        writeQueue.kill()
      }
      reading = false
      connection.destroy()
    }
  }

  function readHead (callback) {
    fs.stat(file, (error, stats) => {
      if (error) return callback(error)
      callback(null, stats.size / lineBytes)
    })
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
    has(argument, 'id', (id) => id.length > 0) &&
    has(argument, 'entry', (entry) => typeof entry === 'object')
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

function sha256 (input) {
  return crypto.createHash('sha256')
    .update(input)
    .digest('hex')
}

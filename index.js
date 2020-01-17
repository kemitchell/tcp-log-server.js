var asyncQueue = require('async.queue')
var duplexJSON = require('duplex-json-stream')
var endOfStream = require('end-of-stream')
var fs = require('fs')
var mkdirp = require('mkdirp')
var path = require('path')
var runSeries = require('run-series')
var split2 = require('split2')
var stringify = require('json-stable-stringify')
var through2 = require('through2')
var uuid = require('uuid').v4

module.exports = function (log, directory, emitter, hashFunction) {
  var logFile = path.join(directory, 'log')
  var entriesPath = path.join(directory, 'entries')

  // Compute the length of a log line.
  var logLineBytes = Buffer.from(hashFunction('') + '\n').length

  return function tcpConnectionHandler (connection) {
    // Connections will be held open long-term, and may sit idle.
    // Enable keep-alive to keep them from dropping.
    connection.setKeepAlive(true)

    // Read the initial length of the log.
    var head
    fs.stat(logFile, function (error, stats) {
      /* istanbul ignore if */
      if (error) {
        if (error.code === 'ENOENT') {
          head = 0
        } else {
          log.error(error)
          return disconnect(error.toString())
        }
      } else {
        head = stats.size / logLineBytes
      }
      log.info({ head: head })
    })

    // Create the subdirectory for entry files.
    mkdirp(entriesPath, function (error) {
      if (error) {
        log.error(error)
        disconnect(error.toString())
      }
    })

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
    //   old entries.
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
      var index
      runSeries([
        function writeEntry (done) {
          fs.writeFile(
            entryPath(hash), content, { flag: 'wx' },
            done
          )
        },
        function appendHashToLog (done) {
          writeLog.info({ event: 'appending', hash: hash })
          var line = hash + '\n'
          fs.writeFile(logFile, line, { flag: 'a' }, done)
        }
      ], function (error) {
        if (error) {
          writeLog.error(error)
          return json.write({
            id: message.id,
            error: error.toString()
          })
        }
        index = ++head
        writeLog.info({ event: 'wrote' })
        // Send confirmation.
        json.write({
          id: message.id,
          index: index
        }, done)
        // Emit an event.
        emitter.emit('entry', index, message.entry, connection)
      })
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
      // Phase 1. Stream entries from the log file, fetching their content
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

      // Phase 1: Stream entries from the log file.
      var streamLog = connectionLog.child({ phase: 'stream' })
      streamLog.info({ event: 'create' })

      var readStream = fs.createReadStream(logFile, {
        encoding: 'utf8',
        start: logLineBytes * (message.from - 1),
        end: logLineBytes * message.read
      })

      var split = split2()

      // Track the highest index seen, so we know when we have sent
      // all the entries requested.
      var highestIndex = message.from - 1

      // For each hash, read the corresponding entry and forward
      // a complete entry object.
      var transform = through2.obj(function (hash, _, done) {
        highestIndex++
        fs.readFile(entryPath(hash), function (error, json) {
          if (error) return onFatalError(error)
          done(null, {
            index: highestIndex,
            entry: JSON.parse(json)
          })
        })
      })

      // Push references so the streams so they can be unpiped and
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
        json.write({ head: head })
        reading = false
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

  function entryPath (hash) {
    return path.join(entriesPath, hash + '.json')
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

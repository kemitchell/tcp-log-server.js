var duplexJSON = require('duplex-json-stream')
var queue = require('async.queue')
var sha256 = require('sha256')
var stringify = require('json-stable-stringify')
var uuid = require('uuid')

var LOG_NAME = 'log'

module.exports = function (serverLog, logs, blobs, emitter) {
  return function (connection) {
    serverLog = serverLog.child({connection: uuid.v4()})
    serverLog.info({
      event: 'connected',
      address: connection.remoteAddress,
      port: connection.removePort
    })

    connection
      .on('end', function () { serverLog.info({event: 'end'}) })
      .on('close', function (error) {
        serverLog.info({event: 'end', error: error})
      })

    // Send JSON back and forth across the connection.
    var json = duplexJSON(connection)
      .on('error', function () {
        serverLog.error({event: 'invalid JSON'})
        reading = false
        emitter.removeListener('entry', onAppend)
        connection.destroy()
      })

    // An object recording information about the state of reading from
    // the log.
    //
    // - doneStreaming (boolean): The server is done streaming old entries.
    //
    // - buffer (array): Contains entries made after the server
    //   started streaming old entries, but before it finished.
    //
    // - stream (stream): The stream of log entries, or null when
    //   doneStreaming is true.
    var reading = false

    // A read advances, in order, through three phases:
    //
    // Phase 1. Streaming entries from the LevelUP store, fetching
    //          their content from the blob store.
    //
    // Phase 2. Sending buffered entries that were written while the
    //          read was streaming.
    //
    // Phase 3. Sending entries as they are written.
    //
    // Comments with "Phase 1", "Phase 2", and "Phase 3" appear in
    // relevant places below.

    // An asynchronous queue for appending hashes to the log. Ensures
    // that each append operation can read the head of the log and
    // number itself appropriately.
    var entriesQueue = queue(function (hash, done) {
      serverLog.info({event: 'appending', hash: hash})
      logs.append(LOG_NAME, hash, done)
    })

    json.on('data', function (message) {
      serverLog.info({event: 'message', message: message})
      if (readMessage(message) && !reading) onReadMessage(message)
      else if (writeMessage(message)) onWriteMessage(message)
      else {
        serverLog.warn({event: 'invalid', message: message})
        json.write({error: 'invalid message'})
      }
    })

    function onReadMessage (message) {
      reading = {doneStreaming: false, buffer: [], from: message.from}
      emitter.addListener('entry', onAppend)

      // Phase 1: Stream index-hash pairs from the LevelUP store.
      var streamLog = serverLog.child({phase: 'stream'})
      streamLog.info({event: 'create'})
      var streamOptions = {since: message.from}
      var stream = logs.createReadStream(LOG_NAME, streamOptions)
      reading.stream = stream
      stream
        .on('data', function (data) {
          var chunks = []
          var errored = false
          // Use the hash from LevelUP to look up the message data in
          // the blob store.
          blobs.createReadStream({key: hashToPath(data.value)})
            .on('data', function (chunk) { chunks.push(chunk) })
            .on('error', function (error) {
              errored = true
              serverLog.error(error)
              json.write({blob: data.value, error: error.toString()})
            })
            .on('end', function () {
              if (!errored) {
                var value = JSON.parse(Buffer.concat(chunks))
                sendEntry(data.seq, value)
              }
            })
        })
        .once('error', function (error) {
          streamLog.error(error)
          sendEntry(-1, {error: error.toString()}, true)
          emitter.removeListener('entry', onAppend)
          reading = false
        })
        .once('end', function () {
          streamLog.info({event: 'end'})
          // Phase 2: Entries may have been written while we were
          // streaming from LevelUP. Send them now.
          reading.buffer.forEach(function (message) {
            sendEntry(message.index, message.key, message.entry)
          })
          // Mark the stream done so messages sent via the
          // EventEmitter will be written out to the socket, rather
          // than buffered.
          reading.buffer = null
          reading.doneStreaming = true
          json.write({current: true})
        })
    }

    function onAppend (index, entry) {
      // Do not send entries from earlier in the log than requested.
      if (reading) {
        if (reading.from <= index) {
          // Phase 3:
          if (reading.doneStreaming) {
            serverLog.info({event: 'forward', index: index})
            sendEntry(index, entry)
          // Waiting for Phase 2
          } else {
            serverLog.info({event: 'buffer', index: index})
            reading.buffer.push({index: index, entry: entry})
          }
        }
      }
    }

    function onWriteMessage (message) {
      var hash = sha256(stringify(message.entry))
      var writeLog = serverLog.child({hash: hash})
      // Append the entry payload in the blob store, by hash.
      blobs.createWriteStream({key: hashToPath(hash)})
        .once('error', function (error) {
          writeLog.error(error)
          json.write({replyTo: message.id, error: error.toString()})
        })
        .once('finish', function () {
          // Append an entry in the LevelUP log with the hash of the payload.
          entriesQueue.push(hash, function (error, index) {
            if (error) {
              writeLog.error(error)
              json.write({replyTo: message.id, error: error.toString()})
            } else {
              writeLog.info({event: 'wrote'})
              json.write({replyTo: message.id, event: 'wrote'})
              // Emit an event.
              emitter.emit('entry', index, message.entry)
            }
          })
        })
        .end(stringify(message.entry), 'utf8') }

    function sendEntry (index, entry, end) {
      json[end ? 'end' : 'write']({index: index, entry: entry})
      serverLog.info({event: 'sent', index: index})
    }
  }
}

function readMessage (argument) {
  return (
    typeof argument === 'object' &&
    has(argument, 'type', 'read') &&
    has(argument, 'from', isPositiveInteger)
  )
}

function writeMessage (argument) {
  return (
    typeof argument === 'object' &&
    has(argument, 'id', function (id) { return id.length > 0 }) &&
    has(argument, 'type', 'write') &&
    has(argument, 'entry', function (entry) {
      return typeof entry === 'object'
    })
  )
}

function has (argument, key, predicate) {
  return (
    argument.hasOwnProperty(key) &&
    typeof predicate === 'function'
      ? predicate(argument[key])
      : argument[key] === predicate
  )
}

function isPositiveInteger (argument) {
  return (
    typeof argument === 'number' &&
    argument % 1 === 0 &&
    argument >= 0
  )
}

function hashToPath (hash) {
  return hash.slice(0, 2) + '/' + hash.slice(2)
}

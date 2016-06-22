var crypto = require('crypto')
var duplexJSON = require('duplex-json-stream')
var hashToPath = require('./hash-to-path')
var stringify = require('json-stable-stringify')
var uuid = require('uuid')
var queue = require('async.queue')

module.exports = function(pino, logs, blobs, emitter) {
  return function(connection) {
    // Child log for this connection.
    var log = pino.child({ connection: uuid.v4() })
    log.info({ event: 'connected' })
    connection
      .on('end', function() { log.info({ event: 'end' }) })
      .on('close', function(error) { log.info({ event: 'end', error: error }) })

    // Send JSON back and forth across the connection.
    var json = duplexJSON(connection)

    // A map tracking logs for which this connection has requested replay.
    //
    // Keys are log name strings.
    //
    // Values are objects with:
    //
    // - doneStreaming (boolean): The server is done streaming old entries.
    //
    // - buffer (array): Contains entries made after the server
    //   started streaming old entries, but before it finished.
    //
    // - stream (stream): The stream of log entries, or null when
    //   doneStreaming is true.
    var replaying = { }

    // New entries are emitted as they are made.
    emitter.on('appended', function(logName, index, entry) {
      // If this connection is replaying the log.
      if (logName in replaying) {
        var replay = replaying[logName]
        // TODO: DO not send entries whose seq < from
        if (replay.doneStreaming) { sendEntry(logName, index, entry) }
        else { replay.buffer.push({ index: index, entry: entry }) } } })

    // An asynchronous queue for appending hashes to logs. Ensures that
    // each append operation can read the head of the log and number
    // itself appropriately.
    var entriesQueue = queue(function(task, done) {
      logs.append(task.log, task.hash, done) })

    json.on('data', function(message) {
      log.info({ event: 'message', message: message })
      if (replayMessage(message)) { onReplayMessage(message) }
      else if(appendMessage(message)) { onAppendMessage(message) } })

    function onReplayMessage(message) {
      var logName = message.log
      var replay = { doneStreaming: false, buffer: [ ] }
      var streamOptions = { since: message.from }
      // A replay advances, in order, through three steps:
      //
      // 1. Streaming entries from the LevelUP store, fetching their
      //    content from the blob store.
      //
      // 2. Sending buffered entries that were appended while the replay
      //    was streaming.
      //
      // 3. Sending entries as they are appended.

      // Stream index-hash pairs from the LevelUP store.
      var stream = logs.createReadStream(logName, streamOptions)
      replay.stream = stream
      replaying[logName] = replay
      stream
        .on('data', function(data) {
          var chunks = [ ]
          var errored = false
          // Use the hash from LevelUP to look up the message data in
          // the blob store.
          blobs.createReadStream({ key: data })
            .on('data', function(chunk) { chunks.push(chunk) })
            .on('error', function(error) {
              errored = true
              log.error(error)
              // TODO: Send an error for the replay, but don't close the connection.
              json.end({
                replyTo: message.id,
                log: message.log,
                error: error.toString() }) })
            .on('end', function() {
              if (!errored) {
                var value = JSON.parse(Buffer.concat(chunks))
                sendEntry(logName, data.seq, value) } }) })
        .once('error', function(error) {
          log.error(error)
          sendEntry(logName, -1, { error: error.toString() })
          delete replaying[logName] })
        .once('end', function() {
          // Entries may have been appended while we were streaming from
          // LevelUP. Send them now.
          replay.buffer.forEach(function(message) {
            sendEntry(logName, message.index, message.key, message.entry) })
          // Mark the stream done so messages sent via the
          // EventEmitter will be written out to the socket, rather
          // than buffered.
          replay.buffer = null
          replaying[logName].doneStreaming = true }) }

    function onAppendMessage(message) {
      var logName = message.log
      var hash = sha256(message.entry)
      // Append the entry payload in the blob store, by hash.
      blobs.createWriteStream({ key: hashToPath(hash) })
        .on('error', function(error) {
          json.write({
            replyTo: message.id,
            log: message.log,
            error: error.toString() }) })
        .on('finish', function() {
          // Append an entry in the LevelUP log with the hash of the payload.
          entriesQueue.push({ log: logName, hash: hash }, function(error, index) {
            if (error) {
              json.write(
                { replyTo: message.id,
                  log: message.log,
                  error: error.toString() }) }
            else {
              json.write(
                { replyTo: message.id,
                  log: message.log,
                  event: 'appended' })
              // Emit an event.
              emitter.emit('appended', logName, index, message.entry) } }) })
        .end(stringify(message.entry), 'utf8') }

    function sendEntry(logName, index, entry) {
      json.write({ log: logName, index: index, entry: entry }) } } }

function replayMessage(argument) {
  return (
    isMessage(argument) &&
    has(argument, 'type', 'replay') &&
    has(argument, 'from', isPositiveInteger) &&
    has(argument, 'log', isString) ) }

function appendMessage(argument) {
  return (
    isMessage(argument) &&
    has(argument, 'type', 'append') &&
    has(argument, 'log', isString) &&
    has(argument, 'entry', function(entry) {
      return ( typeof entry === 'object' ) }) ) }

function isMessage(argument) {
  return (
    ( typeof argument === 'object' ) &&
    has(argument, 'id', function(id) {
      return ( id.length > 0 ) }) ) }

function has(argument, key, predicate) {
  return (
    argument.hasOwnProperty(key) &&
    ( ( typeof predicate === 'function' )
        ? predicate(argument[key])
        : ( argument[key] === predicate ) ) ) }

function isPositiveInteger(argument) {
  return (
    ( typeof argument === 'number' ) &&
    ( argument % 1 === 0 ) &&
    ( argument >= 0 ) ) }

function isString(argument) {
  return (
    ( typeof argument === 'string' ) &&
    ( argument.length > 0 ) ) }

function sha256(argument) {
  return crypto.createHash('sha256')
    .update(stringify(argument))
    .digest('hex') }

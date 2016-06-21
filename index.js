var crypto = require('crypto')
var duplexJSON = require('duplex-json-stream')
var hashToPath = require('hash-to-path')
var uuid = require('uuid')

module.exports = function(pino, logs, blobs, emitter) {
  return function(connection) {
    var log = pino.child({ connection: uuid.v4() })
    log.info({ event: 'connected' })
    connection.on('end', function() {
      log.info({ event: 'end' }) })

    var json = duplexJSON(connection)
    var requests = { }
    json.on('data', function(message) {
      log.info({ event: 'message', message: message })
      var logName = message.log
      if (replayMessage(message)) {
        var request = { done: false, buffer: [ ] }
        emitter.on('messsage', function(toLog, index, message) {
          if (toLog in requests) {
            if (request.done) { send(logName, index, message) }
            else { request.buffer.push({ index: index, message: message }) } } })
        var streamOptions = { since: message.from }
        var stream = logs.createReadStream(logName, streamOptions)
          .on('data', function(data) {
            send(logName, data.seq, data.value) })
          .once('error', function(error) {
            log.error(error)
            send(logName, -1, { error: error.toString() })
            delete requests[logName] })
          .once('end', function() {
            // Send buffered messages.
            request.buffer.forEach(function(message) {
              send(logName, message.index, message.key, message.value) })
            request.buffer = null
            // Mark the stream done so messages sent via the
            // EventEmitter will be written out to the socket, rather
            // than buffered.
            requests[logName].done = true })
        request.stream = stream
        requests[logName] = request }
      else if(storeMessage(message)) {
        logs.append(message.log, message, function(error) {
          if (error) {
            json.write({
              replyTo: message.id,
              log: message.log,
              error: error.toString() }) }
          else {
            var hash = sha256(message)
            blobs.createWriteStream({ key: hashToPath(hash) })
              .end(JSON.stringify(message), 'utf8')
              .on('finish', function() {
                logs.append(message.log, message, function(error) {
                  if (error) {
                    json.write({
                      replyTo: message.id,
                      log: message.log,
                      error: error.toString() }) }
                  else {
                    json.write(
                      { replyTo: message.id,
                        log: message.log,
                        event: 'stored' }) } }) }) } }) } })

    function send(logName, index, message) {
      json.write(
        { log: logName,
          index: index,
          message: message }) } } }

function replayMessage(argument) {
  return (
    isMessage(argument) &&
    has(argument, 'type', 'replay') &&
    has(argument, 'from', isPositiveInteger) &&
    has(argument, 'log', isString) ) }

function storeMessage(argument) {
  return (
    isMessage(argument) &&
    has(argument, 'type', 'store') &&
    has(argument, 'log', isString) &&
    has(argument, 'message', function(message) {
      return ( typeof message === 'object' ) }) ) }

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
    .update(JSON.stringify(argument))
    .digest('hex') }

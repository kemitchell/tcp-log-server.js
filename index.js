var crypto = require('crypto')
var duplexJSON = require('duplex-json-stream')
var hashToPath = require('./hash-to-path')
var stringify = require('json-stable-stringify')
var uuid = require('uuid')

module.exports = function(pino, logs, blobs, emitter) {
  return function(connection) {
    var log = pino.child({ connection: uuid.v4() })
    log.info({ event: 'connected' })
    connection.on('end', function() {
      log.info({ event: 'end' }) })

    var json = duplexJSON(connection)
    var requests = { }
    emitter.on('appended', function(logName, index, entry) {
      if (logName in requests) {
        var request = requests[logName]
        if (request.done) { sendEntry(logName, index, entry) }
        else { request.buffer.push({ index: index, entry: entry }) } } })

    json.on('data', function(message) {
      log.info({ event: 'message', message: message })
      var logName = message.log
      if (replayMessage(message)) {
        var request = { done: false, buffer: [ ] }
        var streamOptions = { since: message.from }
        var stream = logs.createReadStream(logName, streamOptions)
        request.stream = stream
        requests[logName] = request
        stream
          .on('data', function(data) {
            console.log('%s is %j', 'data', data)
            sendEntry(logName, data.seq, data.value) })
          .once('error', function(error) {
            log.error(error)
            sendEntry(logName, -1, { error: error.toString() })
            delete requests[logName] })
          .once('end', function() {
            // Send buffered messages.
            request.buffer.forEach(function(message) {
              sendEntry(logName, message.index, message.key, message.entry) })
            request.buffer = null
            // Mark the stream done so messages sent via the
            // EventEmitter will be written out to the socket, rather
            // than buffered.
            requests[logName].done = true }) }
      else if(storeMessage(message)) {
        var hash = sha256(message.entry)
        blobs.createWriteStream({ key: hashToPath(hash) })
          .on('error', function(error) {
            json.write({
              replyTo: message.id,
              log: message.log,
              error: error.toString() }) })
          .on('finish', function() {
            console.log('appending')
            console.log('%s is %j', 'message.log', message.log)
            setImmediate(function() {
              logs.append(message.log, hash, function(error, index) {
                console.log('%s is %j', 'index', index)
                if (error) {
                  json.write(
                    { replyTo: message.id,
                      log: message.log,
                      error: error.toString() }) }
                else {
                  json.write(
                    { replyTo: message.id,
                      log: message.log,
                      event: 'stored' })
                  console.log('%s is %j', 'message.entry', message.entry)
                  emitter.emit('appended', logName, index, message.entry) } }) }) })
          .end(stringify(message.entry), 'utf8') } })

    function sendEntry(logName, index, entry) {
      json.write({ log: logName, index: index, entry: entry }) } } }

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

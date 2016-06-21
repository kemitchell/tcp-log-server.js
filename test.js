var devnull = require('dev-null')
var duplexJSON = require('duplex-json-stream')
var levelLogs = require('level-logs')
var levelup = require('levelup')
var memdown = require('memdown')
var net = require('net')
var pino = require('pino')
var tape = require('tape')
var uuid = require('uuid')

tape('simple sync', function(test) {
  var level = levelup('', { db: memdown })
  var logs = levelLogs(level, { valueEncoding: 'json' })
  var blobs = require('abstract-blob-store')()
  var log = pino({ }, devnull())
  var emitter = new (require('events').EventEmitter)()
  var handler = require('./')(log, logs, blobs, emitter)
  var messageCount = 0
  var server = net.createServer()
    .on('connection', handler)
    .listen(0, function() {
      var serverPort = this.address().port
      var client = net.connect(serverPort)
      var clientJSON = duplexJSON(client)
      var storeUUID = uuid.v4()
      var replayUUID = uuid.v4()
      clientJSON
        .on('data', function(data) {
          messageCount++
          if (messageCount ===  1) {
            test.deepEqual(
              data,
              { event: 'stored',
                log: 'test',
                replyTo: storeUUID },
              'first message confirms append') }
          else if (messageCount === 2) {
            test.deepEqual(
              data,
              { log: 'test',
                index: 1,
                entry: { a: 1 } },
              'second message replays entry')
            client.end()
            server.close()
            level.close()
            test.end() } })
      clientJSON.write(
        { type: 'store',
          log: 'test',
          entry: { a: 1 },
          id: storeUUID })
      clientJSON.write(
        { type: 'replay',
          log: 'test',
          from: 0,
          id: replayUUID }) }) })

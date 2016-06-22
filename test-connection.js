var devnull = require('dev-null')
var duplexJSON = require('duplex-json-stream')
var levelLogs = require('level-logs')
var levelup = require('levelup')
var memdown = require('memdown')
var net = require('net')
var pino = require('pino')

module.exports = function(numberOfClients, callback) {
  memdown.clearGlobalStore()
  var level = levelup('', { db: memdown })
  var logs = levelLogs(level, { valueEncoding: 'json' })
  var blobs = require('abstract-blob-store')()
  var log = pino({ }, devnull())
  var emitter = new (require('events').EventEmitter)()
  var handler = require('./')(log, logs, blobs, emitter)
  var server = net.createServer()
    .on('connection', handler)
    .on('close', function() {
      level.close() })
    .listen(0, function() {
      var serverPort = this.address().port
      var clients = []
      for (var n = 0; n < numberOfClients; n++) {
        var client = net.connect(serverPort)
        var clientJSON = duplexJSON(client)
        clients.push(clientJSON) }
      if (numberOfClients === 1) {
        callback(clients[0], server) }
      else {
        callback(clients, server) } }) }

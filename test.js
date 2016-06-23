var deepEqual = require('deep-equal')
var devnull = require('dev-null')
var duplexJSON = require('duplex-json-stream')
var levelLogs = require('level-logs')
var levelup = require('levelup')
var memdown = require('memdown')
var net = require('net')
var pino = require('pino')
var tape = require('tape')
var uuid = require('uuid').v4

tape('simple sync', function(test) {
  testConnections(1, function(client, server) {
    var log = 'test'
    var appendUUID = uuid()
    var readUUID = uuid()
    var messageCount = 0
    client.on('data', function(data) {
      messageCount++
      if (messageCount ===  1) {
        test.deepEqual(
          data,
          { event: 'appended', log: log, replyTo: appendUUID },
          'first message confirms append') }
      else if (messageCount === 2) {
        test.deepEqual(
          data,
          { log: log, index: 1, entry: { a: 1 } },
          'second message reads entry')
        client.end()
        server.close()
        test.end() } })
    client.write({ log: log, type: 'append', entry: { a: 1 }, id: appendUUID })
    client.write({ log: log, type: 'read', from: 0, id: readUUID }) }) })

tape('writes before and after read', function(test) {
  testConnections(1, function(client, server) {
    var log = 'test'
    var firstAppendUUID = uuid()
    var secondAppendUUID = uuid()
    var readUUID = uuid()
    var messageCount = 0
    var messages = [ ]
    client.on('data', function(data) {
      messageCount++
      messages.push(data)
      if (messageCount === 4) {
        test.assert(
          messages.some(function(element) {
            return deepEqual(
              element,
              { log: log, index: 1, entry: { a: 1 } }) }),
          'reads first entry')
        test.assert(
          messages.some(function(element) {
            return deepEqual(
              element,
              { log: log, index: 2, entry: { b: 2 } }) }),
          'reads second entry')
        client.end()
        server.close()
        test.end() } })
    client.write({ log: log, type: 'append', entry: { a: 1 }, id: firstAppendUUID })
    client.write({ log: log, type: 'read', from: 0, id: readUUID })
    client.write({ log: log, type: 'append', entry: { b: 2 }, id: secondAppendUUID }) }) })

tape('two clients', function(test) {
  testConnections(2, function(clients, server) {
    var log = 'test'
    var ana = clients[0]
    var bob = clients[1]
    var anaWasHere = { message: 'Ana was here.' }
    var bobWasHere = { message: 'Bob was here.' }
    var finished = 0
    function finish() {
      if (++finished === 2) {
        server.close()
        test.end() } }
    ana.on('data', function(data) {
      if (data.log === log && deepEqual(data.entry, bobWasHere)) {
        test.pass('receives entry from other client')
        ana.end()
        finish() } })
    bob.on('data', function(data) {
      if (data.log === log && deepEqual(data.entry, anaWasHere)) {
        test.pass('receives entry from other client')
        bob.end()
        finish() } })
    ana.write({ log: log, type: 'read', from: 0, id: uuid() })
    bob.write({ log: log, type: 'read', from: 0, id: uuid() })
    ana.write({ log: log, type: 'append', entry: anaWasHere, id: uuid() })
    bob.write({ log: log, type: 'append', entry: bobWasHere, id: uuid() }) }) })

tape('old entry', function(test) {
  testConnections(1, function(client, server) {
    var log = 'test'
    var entry = { a: 1 }
    client.on('data', function(data) {
      if (data.log === log && deepEqual(data.entry, entry)) {
        test.pass('receives old entry')
        client.end()
        server.close()
        test.end() } })
    client.write({ log: log, type: 'append', entry: entry, id: uuid() })
    client.write({ log: log, type: 'read', from: 0, id: uuid() }) }) })

tape('read from future index', function(test) {
  testConnections(1, function(client, server) {
    var log = 'test'
    var entries = [ { a: 1 }, { b: 2 } ]
    client.on('data', function(data) {
      if (data.log === log) {
        if (deepEqual(data.entry, entries[0])) {
          test.fail('received earlier entry') }
        else if (deepEqual(data.entry, entries[1])) {
          test.pass('receives newer entry')
          client.end()
          server.close()
          test.end() } } })
    client.write({ log: log, type: 'read', from: 2, id: uuid() })
    client.write({ log: log, type: 'append', entry: entries[0], id: uuid() })
    client.write({ log: log, type: 'append', entry: entries[1], id: uuid() }) }) })

function testConnections(numberOfClients, callback) {
  memdown.clearGlobalStore()
  // Use an in-memory storage back-end.
  var level = levelup('', { db: memdown })
  var logs = levelLogs(level, { valueEncoding: 'json' })
  // Use an in-memory blob store.
  var blobs = require('abstract-blob-store')()
  // Pipe log messages to nowhere.
  var log = pino({ }, devnull())
  var emitter = new (require('events').EventEmitter)()
  var handler = require('./')(log, logs, blobs, emitter)
  var server = net.createServer()
    .on('connection', handler)
    .on('close', function() { level.close() })
    .listen(0, function() {
      var serverPort = this.address().port
      var clients = []
      for (var n = 0; n < numberOfClients; n++) {
        var client = net.connect(serverPort)
        var clientJSON = duplexJSON(client)
        clients.push(clientJSON) }
      if (numberOfClients === 1) { callback(clients[0], server) }
      else { callback(clients, server) } }) }

var deepEqual = require('deep-equal')
var devnull = require('dev-null')
var duplexJSON = require('duplex-json-stream')
var levelLogs = require('level-logs')
var levelup = require('levelup')
var memdown = require('memdown')
var net = require('net')
var pino = require('pino')
var tape = require('tape')

simpleTest('confirms writes', {
  send: [{type: 'write', entry: {a: 1}, id: 'abc123'}],
  receive: [{event: 'wrote', id: 'abc123', index: 1}]
})

simpleTest('duplicate read', {
  send: [
    {type: 'read', from: 0},
    {type: 'read', from: 0}
  ],
  receive: [
    {error: 'already reading'}
  ]
})

simpleTest('simple sync', {
  send: [
    {type: 'write', entry: {a: 1}, id: 'abc123'},
    {type: 'read', from: 0}
  ],
  receive: [
    {current: true},
    {event: 'wrote', id: 'abc123', index: 1}
  ]
})

tape('writes before and after read', function (test) {
  testConnections(1, function (client, server) {
    var messages = []
    var expected = [
      {current: true},
      {event: 'wrote', id: 'first', index: 1},
      {event: 'wrote', id: 'second', index: 2}
    ]
    client.on('data', function (data) {
      messages.push(data)
      if (messages.length === expected.length) {
        test.deepEqual(messages, expected)
        client.end()
        server.close()
        test.end()
      }
    })
    client.write({type: 'write', entry: {a: 1}, id: 'first'})
    client.write({type: 'read', from: 0})
    client.write({type: 'write', entry: {b: 2}, id: 'second'})
  })
})

simpleTest('writes before and after read', {
  send: [
    {type: 'write', entry: {a: 1}, id: 'first write'},
    {type: 'read', from: 0},
    {type: 'write', entry: {b: 2}, id: 'second write'}
  ],
  receive: [
    {current: true},
    {event: 'wrote', id: 'first write', index: 1},
    {event: 'wrote', id: 'second write', index: 2}
  ]
})

tape('two clients', function (test) {
  testConnections(2, function (clients, server) {
    var ana = clients[0]
    var bob = clients[1]
    var anaWasHere = {message: 'Ana was here.'}
    var bobWasHere = {message: 'Bob was here.'}

    var anaFinished = false
    var bobFinished = false
    function finish () {
      if (anaFinished && bobFinished) {
        server.close()
        test.end()
      }
    }
    ana.on('data', function (data) {
      if (deepEqual(data.entry, bobWasHere)) {
        test.pass('receives entry from other client')
        ana.end()
        anaFinished = true
        finish()
      }
    })
    bob.on('data', function (data) {
      if (deepEqual(data.entry, anaWasHere)) {
        test.pass('receives entry from other client')
        bob.end()
        bobFinished = true
        finish()
      }
    })
    ana.write({type: 'read', from: 0})
    bob.write({type: 'read', from: 0})
    ana.write({type: 'write', entry: anaWasHere, id: 'first'})
    bob.write({type: 'write', entry: bobWasHere, id: 'second'})
  })
})

tape('old entry', function (test) {
  testConnections(1, function (client, server) {
    var entry = {a: 1}
    client.on('data', function (data) {
      if (deepEqual(data.entry, entry)) {
        test.pass('receives old entry')
        client.end()
        server.close()
        test.end()
      }
    })
    client.write({type: 'write', entry: entry, id: 'first'})
    setTimeout(function () {
      client.write({type: 'read', from: 0})
    }, 25)
  })
})

tape('read from future index', function (test) {
  testConnections(2, function (clients, server) {
    var readingClient = clients[0]
    var writingClient = clients[1]
    var entries = [{a: 1}, {b: 2}]
    readingClient.on('data', function (data) {
      /* istanbul ignore if */
      if (deepEqual(data.entry, entries[0])) {
        test.fail('received earlier entry')
      } else if (deepEqual(data.entry, entries[1])) {
        test.pass('receives newer entry')
        readingClient.end()
        server.close()
        test.end()
      }
    })
    readingClient.write({type: 'read', from: 2})
    writingClient.write({type: 'write', entry: entries[0], id: 'first'})
    writingClient.write({type: 'write', entry: entries[1], id: 'second'})
    writingClient.end()
  })
})

tape('current signal', function (test) {
  testConnections(2, function (clients, server) {
    var readingClient = clients[0]
    var writingClient = clients[1]
    var entries = [{a: 1}, {b: 2}, {c: 3}]
    var messages = []
    var expected = [
      {index: 1, entry: entries[0]},
      {index: 2, entry: entries[1]},
      {current: true},
      {index: 3, entry: entries[2]}
    ]
    readingClient.on('data', function (data) {
      messages.push(data)
      if (messages.length === expected.length) {
        test.deepEqual(messages, expected)
        readingClient.end()
        server.close()
        test.end()
      }
    })
    writingClient.write({type: 'write', entry: entries[0], id: 'first'})
    writingClient.write({type: 'write', entry: entries[1], id: 'second'})
    setTimeout(function () {
      readingClient.write({type: 'read', from: 0})
      setTimeout(function () {
        writingClient.end({type: 'write', entry: entries[2], id: 'third'})
      }, 50)
    }, 50)
  })
})

tape('invalid message', function (test) {
  testConnections(1, function (client, server) {
    client.on('data', function (data) {
      test.deepEqual(data, {error: 'invalid message'})
      client.end()
      server.close()
      test.end()
    })
    client.write({type: 'nonsense'})
  })
})

tape('invalid json', function (test) {
  testConnections(0, function (_, server, port) {
    var client = net.connect({port: port})
      .on('connect', function () { client.write('not JSON\n') })
      .on('close', function () {
        test.pass('connection closed by server')
        server.close()
        test.end()
      })
  })
})

function simpleTest (name, options) {
  tape(name, function (test) {
    testConnections(1, function (client, server) {
      var expected = options.receive
      var received = []
      client.on('data', function (data) {
        received.push(data)
        if (received.length === expected.length) {
          setTimeout(function () {
            test.deepEqual(received, expected)
            client.end()
            server.close()
            test.end()
          }, 100)
        } else if (received.length > expected.length) {
          test.fail('too many messages received')
        }
      })
      options.send.forEach(function (message) {
        client.write(message)
      })
    })
  })
}

function testConnections (numberOfClients, callback) {
  memdown.clearGlobalStore()
  // Use an in-memory storage back-end.
  var level = levelup('', {db: memdown})
  var logs = levelLogs(level, {valueEncoding: 'json'})
  // Use an in-memory blob store.
  var blobs = require('abstract-blob-store')()
  // Pipe log messages to nowhere.
  var log = pino({}, devnull())
  var emitter = new (require('events').EventEmitter)()
  var handler = require('./')(log, logs, blobs, emitter)
  var server = net.createServer()
    .on('connection', handler)
    .once('close', function () { level.close() })
    .listen(0, function () {
      var serverPort = this.address().port
      var clients = []
      for (var n = 0; n < numberOfClients; n++) {
        var client = net.connect(serverPort)
        var clientJSON = duplexJSON(client)
        clients.push(clientJSON)
      }
      if (numberOfClients === 1) callback(clients[0], server, serverPort)
      else callback(clients, server, serverPort)
    })
}

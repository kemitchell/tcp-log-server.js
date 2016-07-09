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
  send: [{entry: {a: 1}, id: 'abc123'}],
  receive: [{id: 'abc123', index: 1}]
})

simpleTest('duplicate read', {
  send: [{from: 1}, {from: 1}],
  receive: [{error: 'already reading'}]
})

simpleTest('simple sync', {
  send: [
    {entry: {a: 1}, id: 'abc123'},
    {from: 1}
  ],
  receive: [
    {current: true},
    {id: 'abc123', index: 1}
  ]
})

tape('writes before and after read', function (test) {
  testConnections(1, function (client, server) {
    var messages = []
    var expected = [
      {current: true},
      {id: 'first', index: 1},
      {id: 'second', index: 2}
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
    client.write({entry: {a: 1}, id: 'first'})
    client.write({from: 1})
    client.write({entry: {b: 2}, id: 'second'})
  })
})

simpleTest('writes before and after read', {
  send: [
    {entry: {a: 1}, id: 'first write'},
    {from: 1},
    {entry: {b: 2}, id: 'second write'}
  ],
  receive: [
    {current: true},
    {id: 'first write', index: 1},
    {id: 'second write', index: 2}
  ]
})

tape('buffering', function (test) {
  testConnections(2, function (clients, server) {
    var reading = clients[0]
    var writing = clients[1]
    var received = []
    var receivedCurrent = false
    reading.on('data', function (data) {
      if (data.current) receivedCurrent = true
      else received.push(data)
      if (received.length === 200) {
        test.pass('received 200 entries')
        test.assert(received.every(function (element, index) {
          console.log('%s is %j', 'element', element)
          return element.entry.i === index
        }), 'received entries in order')
        test.assert(receivedCurrent, 'received current')
        reading.end()
        writing.end()
        server.close()
        test.end()
      }
    })
    // Append 100 entries, so the reading client's request will trigger a
    // long-running LevelUP read stream.
    var counter = 0
    for (; counter < 100; counter++) {
      writing.write({entry: {i: counter}, id: counter.toString()})
    }
    // Wait for the log server to write the entries to its LevelUP.
    setTimeout(function () {
      // Request a read.
      reading.write({from: 1})
      setImmediate(function () {
        // Append another 100 entries. Some of these should reach the server
        // while it is streaming from LevelUP to respond to the reading client.
        // That triggers buffering.
        for (; counter < 200; counter++) {
          writing.write({entry: {i: counter}, id: counter.toString()})
        }
      })
    }, 250)
  })
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
    ana.write({from: 1})
    bob.write({from: 1})
    ana.write({entry: anaWasHere, id: 'first'})
    bob.write({entry: bobWasHere, id: 'second'})
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
    client.write({entry: entry, id: 'first'})
    setTimeout(function () {
      client.write({from: 1})
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
    readingClient.write({from: 2})
    writingClient.write({entry: entries[0], id: 'first'})
    writingClient.write({entry: entries[1], id: 'second'})
    writingClient.end()
  })
})

tape('read from later', function (test) {
  testConnections(2, function (clients, server) {
    var readingClient = clients[0]
    var writingClient = clients[1]
    var entries = [{a: 1}, {b: 2}, {c: 3}]
    var received = []
    readingClient.on('data', function (data) {
      if ('entry' in data) received.push(data.entry)
      if (received.length === 2) {
        test.deepEqual(
          received, entries.slice(1),
          'received last two')
        readingClient.end()
        server.close()
        test.end()
      }
    })
    readingClient.write({from: 2})
    entries.forEach(function (entry, index) {
      writingClient.write({entry: entry, id: '' + index})
    })
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
    writingClient.write({entry: entries[0], id: 'first'})
    writingClient.write({entry: entries[1], id: 'second'})
    setTimeout(function () {
      readingClient.write({from: 1})
      setTimeout(function () {
        writingClient.end({entry: entries[2], id: 'third'})
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

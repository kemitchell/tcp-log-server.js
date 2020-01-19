const deepEqual = require('deep-equal')
const devnull = require('dev-null')
const duplexJSON = require('duplex-json-stream')
const fs = require('fs')
const net = require('net')
const os = require('os')
const path = require('path')
const pino = require('pino')
const rimraf = require('rimraf')
const tape = require('tape')

tape('confirm writes', (test) => {
  simpleTest({
    send: [{ entry: { a: 1 }, id: 'abc123' }],
    receive: [{ id: 'abc123', index: 1 }]
  }, test)
})

tape.skip('duplicate read', (test) => {
  simpleTest({
    send: [
      { from: 1, read: 1 },
      { from: 1, read: 1 }
    ],
    receive: [
      { error: 'already reading' },
      { current: true }
    ]
  }, test)
})

tape('simple sync', (test) => {
  simpleTest({
    send: [
      { entry: { a: 1 }, id: 'abc123' },
      { from: 1, read: 1 }
    ],
    receive: [
      { current: true },
      { id: 'abc123', index: 1 },
      { index: 1, entry: { a: 1 } },
      { head: 1 }
    ]
  }, test)
})

tape('writes before and after read', (test) => {
  testConnections(1, (client, server) => {
    const messages = []
    const expected = [
      { current: true },
      { id: 'first', index: 1 },
      { index: 1, entry: { a: 1 } },
      { id: 'second', index: 2 },
      { index: 2, entry: { b: 2 } },
      { head: 2 }
    ]
    client.on('data', (data) => {
      messages.push(data)
      if (messages.length === expected.length) {
        test.deepEqual(messages, expected)
        client.end()
        server.close()
        test.end()
      }
    })
    client.write({ entry: { a: 1 }, id: 'first' })
    client.write({ from: 1, read: 2 })
    client.write({ entry: { b: 2 }, id: 'second' })
  })
})

tape('writes before and after read', (test) => {
  simpleTest({
    send: [
      { entry: { a: 1 }, id: 'first write' },
      { from: 1, read: 2 },
      { entry: { b: 2 }, id: 'second write' }
    ],
    receive: [
      { current: true },
      { id: 'first write', index: 1 },
      { index: 1, entry: { a: 1 } },
      { id: 'second write', index: 2 },
      { index: 2, entry: { b: 2 } },
      { head: 2 }
    ]
  }, test)
})

tape('buffering', (test) => {
  testConnections(2, (clients, server) => {
    const reading = clients[0]
    const writing = clients[1]
    const received = []
    let receivedCurrent = false
    reading.on('data', (data) => {
      if (data.current) receivedCurrent = true
      else received.push(data)
      if (received.length === 200) {
        test.pass('received 200 entries')
        test.assert(received.every((element, index) => {
          return element.entry.i === index
        }), 'received entries in order')
        test.assert(receivedCurrent, 'received current')
        reading.end()
        writing.end()
        server.close()
        test.end()
      }
    })
    // Append 100 entries, so the reading client's request will trigger
    // a long-running LevelUP read stream.
    let counter = 0
    for (; counter < 100; counter++) {
      writing.write({ entry: { i: counter }, id: counter.toString() })
    }
    // Wait for the log server to write the entries to its LevelUP.
    setTimeout(() => {
      // Request a read.
      reading.write({ from: 1, read: 200 })
      setImmediate(() => {
        // Append another 100 entries. Some of these should reach the
        // server while it is streaming from LevelUP to respond to the
        // reading client. That triggers buffering.
        for (; counter < 200; counter++) {
          writing.write({ entry: { i: counter }, id: counter.toString() })
        }
      })
    }, 250)
  })
})

tape('buffering limited pull', (test) => {
  testConnections(2, (clients, server) => {
    const reading = clients[0]
    const writing = clients[1]
    const received = []
    reading.on('data', (data) => {
      received.push(data)
      if (received.length === 50) {
        test.pass('received 200 entries')
        test.assert(received.every((element, index) => {
          return element.entry.i === index
        }), 'received entries in order')
        reading.end()
        writing.end()
        server.close()
        test.end()
      }
    })
    // Append 100 entries, so the reading client's request will trigger
    // a long-running LevelUP read stream.
    let counter = 0
    for (; counter < 100; counter++) {
      writing.write({ entry: { i: counter }, id: counter.toString() })
    }
    // Wait for the log server to write the entries to its LevelUP.
    setTimeout(() => {
      // Request a read.
      reading.write({ from: 1, read: 50 })
      setImmediate(() => {
        // Append another 100 entries. Some of these should reach the
        // server while it is streaming from LevelUP to respond to the
        // reading client. That triggers buffering.
        for (; counter < 200; counter++) {
          writing.write({ entry: { i: counter }, id: counter.toString() })
        }
      })
    }, 250)
  })
})

tape('two clients', (test) => {
  testConnections(2, (clients, server) => {
    const ana = clients[0]
    const bob = clients[1]
    const anaWasHere = { message: 'Ana was here.' }
    const bobWasHere = { message: 'Bob was here.' }

    let anaFinished = false
    let bobFinished = false
    function finish () {
      if (anaFinished && bobFinished) {
        server.close()
        test.end()
      }
    }
    ana.on('data', (data) => {
      if (deepEqual(data.entry, bobWasHere)) {
        test.pass('receives entry from other client')
        ana.end()
        anaFinished = true
        finish()
      }
    })
    bob.on('data', (data) => {
      if (deepEqual(data.entry, anaWasHere)) {
        test.pass('receives entry from other client')
        bob.end()
        bobFinished = true
        finish()
      }
    })
    ana.write({ from: 1, read: 2 })
    bob.write({ from: 1, read: 2 })
    ana.write({ entry: anaWasHere, id: 'first' })
    bob.write({ entry: bobWasHere, id: 'second' })
  })
})

tape('old entry', (test) => {
  testConnections(1, (client, server) => {
    const entry = { a: 1 }
    client.on('data', (data) => {
      if (deepEqual(data.entry, entry)) {
        test.pass('receives old entry')
        client.end()
        server.close()
        test.end()
      }
    })
    client.write({ entry: entry, id: 'first' })
    setTimeout(() => {
      client.write({ from: 1, read: 1 })
    }, 25)
  })
})

tape('read from future index', (test) => {
  testConnections(2, (clients, server) => {
    const readingClient = clients[0]
    const writingClient = clients[1]
    const entries = [{ a: 1 }, { b: 2 }]
    readingClient.on('data', (data) => {
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
    readingClient.write({ from: 2, read: 1 })
    writingClient.write({ entry: entries[0], id: 'first' })
    writingClient.write({ entry: entries[1], id: 'second' })
    writingClient.end()
  })
})

tape('read from later', (test) => {
  testConnections(2, (clients, server) => {
    const readingClient = clients[0]
    const writingClient = clients[1]
    const entries = [{ a: 1 }, { b: 2 }, { c: 3 }]
    const received = []
    readingClient.on('data', (data) => {
      if ('entry' in data) {
        received.push(data.entry)
        if (received.length === 2) {
          test.deepEqual(
            received, entries.slice(1),
            'received last two')
          readingClient.end()
          server.close()
          test.end()
        }
      }
    })
    readingClient.write({ from: 2, read: 2 })
    entries.forEach((entry, index) => {
      writingClient.write({ entry: entry, id: '' + index })
    })
    writingClient.end()
  })
})

tape('current signal', (test) => {
  testConnections(2, (clients, server) => {
    const readingClient = clients[0]
    const writingClient = clients[1]
    const entries = [{ a: 1 }, { b: 2 }, { c: 3 }]
    const messages = []
    const expected = [
      { index: 1, entry: entries[0] },
      { index: 2, entry: entries[1] },
      { current: true },
      { index: 3, entry: entries[2] },
      { head: 3 }
    ]
    readingClient.on('data', (data) => {
      messages.push(data)
      if (messages.length === expected.length) {
        test.deepEqual(messages, expected)
        readingClient.end()
        server.close()
        test.end()
      }
    })
    writingClient.write({ entry: entries[0], id: 'first' })
    writingClient.write({ entry: entries[1], id: 'second' })
    setTimeout(() => {
      readingClient.write({ from: 1, read: entries.length })
      setTimeout(() => {
        writingClient.end({ entry: entries[2], id: 'third' })
      }, 50)
    }, 50)
  })
})

tape('successive reads', (test) => {
  testConnections(2, (clients, server) => {
    const readingClient = clients[0]
    const writingClient = clients[1]
    const entries = [{ a: 1 }, { b: 2 }, { c: 3 }]
    const received = []
    const expected = [
      { index: 1, entry: entries[0] },
      { head: 3 },
      { index: 2, entry: entries[1] },
      { head: 3 },
      { index: 3, entry: entries[2] },
      { head: 3 },
      { current: true }
    ]
    let lastSeen = 0
    readingClient.on('data', function (data) {
      received.push(data)
      if (data.index) {
        lastSeen = data.index
      } else if (data.head) {
        readingClient.write({ from: lastSeen + 1, read: 1 })
      }
      if (received.length === expected.length) {
        this.removeAllListeners()
        test.deepEqual(received, expected)
        writingClient.end()
        readingClient.end()
        server.close()
        test.end()
      }
    })
    entries.forEach((entry, index) => {
      writingClient.write({ entry: entry, id: String(index) })
    })
    setTimeout(() => {
      readingClient.write({ from: 1, read: 1 })
    }, 100)
  })
})

tape('invalid message', (test) => {
  testConnections(1, (client, server) => {
    client.on('data', (data) => {
      test.deepEqual(data, { error: 'invalid message' })
      client.end()
      server.close()
      test.end()
    })
    client.write({ type: 'nonsense' })
  })
})

tape('invalid json', (test) => {
  testConnections(1, (client, server, port) => {
    client.socket
      .once('connect', () => { client.socket.write('not JSON\n') })
      .once('close', () => {
        test.pass('connection closed by server')
        server.close()
        test.end()
      })
  })
})

tape('invalid json during read', (test) => {
  testConnections(1, (client, server, port) => {
    client.socket
      .once('connect', () => {
        client.write({ from: 1, read: 1 })
        client.socket.write('not JSON\n')
      })
      .once('close', () => {
        test.pass('connection closed by server')
        server.close()
        test.end()
      })
  })
})

function simpleTest (options, test) {
  testConnections(1, (client, server) => {
    const expected = options.receive
    const received = []
    client.on('data', (data) => {
      received.push(data)
      if (received.length === expected.length) {
        setTimeout(() => {
          test.deepEqual(received, expected)
          client.end()
          server.close()
          test.end()
        }, 100)
      } else {
        /* istanbul ignore next */
        if (received.length > expected.length) {
          test.fail('too many messages received')
        }
      }
    })
    options.send.forEach((message) => {
      client.write(message)
    })
  })
}

function testConnections (numberOfClients, callback) {
  // Use a temporary file.
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'tcp-log-server-'))
  const file = path.join(directory, 'log')
  fs.writeFileSync(file, '')
  // Use an in-memory blob store.
  const blobs = require('abstract-blob-store')()
  // Pipe log messages to nowhere.
  const log = pino({}, devnull())
  const emitter = new (require('events').EventEmitter)()
  const handler = require('./')({ log, file, blobs, emitter })
  const server = net.createServer()
    .on('connection', handler)
    .once('close', () => { rimraf.sync(directory) })
    .listen(0, function () {
      const serverPort = this.address().port
      const clients = []
      for (let n = 0; n < numberOfClients; n++) {
        const client = net.connect(serverPort)
        const clientJSON = duplexJSON(client)
        clientJSON.socket = client
        clients.push(clientJSON)
      }
      if (numberOfClients === 1) {
        callback(clients[0], server, serverPort)
      } else {
        callback(clients, server, serverPort)
      }
    })
}

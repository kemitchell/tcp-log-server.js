var tape = require('tape')
var testConnections = require('./test-connections')
var uuid = require('uuid').v4
var deepEqual = require('deep-equal')

tape('simple sync', function(test) {
  testConnections(1, function(client, server) {
    var log = 'test'
    var appendUUID = uuid()
    var replayUUID = uuid()
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
          'second message replays entry')
        client.end()
        server.close()
        test.end() } })
    client.write({ log: log, type: 'append', entry: { a: 1 }, id: appendUUID })
    client.write({ log: log, type: 'replay', from: 0, id: replayUUID }) }) })

tape('writes before and after replay', function(test) {
  testConnections(1, function(client, server) {
    var log = 'test'
    var firstAppendUUID = uuid()
    var secondAppendUUID = uuid()
    var replayUUID = uuid()
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
          'replays first entry')
        test.assert(
          messages.some(function(element) {
            return deepEqual(
              element,
              { log: log, index: 2, entry: { b: 2 } }) }),
          'replays second entry')
        client.end()
        server.close()
        test.end() } })
    client.write({ log: log, type: 'append', entry: { a: 1 }, id: firstAppendUUID })
    client.write({ log: log, type: 'replay', from: 0, id: replayUUID })
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
    ana.write({ log: log, type: 'replay', from: 0, id: uuid() })
    bob.write({ log: log, type: 'replay', from: 0, id: uuid() })
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
    client.write({ log: log, type: 'replay', from: 0, id: uuid() }) }) })

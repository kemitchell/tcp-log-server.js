var tape = require('tape')
var testConnections = require('./test-connections')
var uuid = require('uuid').v4
var deepEqual = require('deep-equal')

tape('simple sync', function(test) {
  testConnections(1, function(client, server) {
    var storeUUID = uuid()
    var replayUUID = uuid()
    var messageCount = 0
    client.on('data', function(data) {
      messageCount++
      if (messageCount ===  1) {
        test.deepEqual(
          data,
          { event: 'stored', log: 'test', replyTo: storeUUID },
          'first message confirms append') }
      else if (messageCount === 2) {
        test.deepEqual(
          data,
          { log: 'test', index: 1, entry: { a: 1 } },
          'second message replays entry')
        client.end()
        server.close()
        test.end() } })
    client.write(
      { type: 'store', log: 'test', entry: { a: 1 }, id: storeUUID })
    client.write(
      { type: 'replay', log: 'test', from: 0, id: replayUUID }) }) })

tape('writes before and after replay', function(test) {
  testConnections(1, function(client, server) {
    var firstStoreUUID = uuid()
    var secondStoreUUID = uuid()
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
              { log: 'test', index: 1, entry: { a: 1 } }) }),
          'replays first entry')
        test.assert(
          messages.some(function(element) {
            return deepEqual(
              element,
              { log: 'test', index: 2, entry: { b: 2 } }) }),
          'replays second entry')
        client.end()
        server.close()
        test.end() } })
    client.write(
      { type: 'store', log: 'test', entry: { a: 1 }, id: firstStoreUUID })
    client.write(
      { type: 'replay', log: 'test', from: 0, id: replayUUID })
    client.write(
      { type: 'store', log: 'test', entry: { b: 2 }, id: secondStoreUUID }) }) })

tape('two clients', function(test) {
  testConnections(2, function(clients, server) {
    var sharedLog = 'test'
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
      if (data.log === sharedLog && deepEqual(data.entry, bobWasHere)) {
        test.pass('receives entry from second client')
        ana.end()
        finish() } })
    bob.on('data', function(data) {
      if (data.log === sharedLog && deepEqual(data.entry, anaWasHere)) {
        test.pass('receives entry from first client')
        bob.end()
        finish() } })
    ana.write(
      { type: 'replay', log: sharedLog, from: 0, id: uuid() })
    bob.write(
      { type: 'replay', log: sharedLog, from: 0, id: uuid() })
    ana.write(
      { type: 'store', log: sharedLog, entry: anaWasHere, id: uuid() })
    bob.write(
      { type: 'store', log: sharedLog, entry: bobWasHere, id: uuid() }) }) })

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
    client.write(
      { type: 'store', log: log, entry: entry, id: uuid() })
    client.write(
      { type: 'replay', log: log, from: 0, id: uuid() }) }) })

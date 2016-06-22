var tape = require('tape')
var testConnection = require('./test-connection')
var uuid = require('uuid')
var deepEqual = require('deep-equal')

tape('simple sync', function(test) {
  testConnection(function(client, server) {
    var storeUUID = uuid.v4()
    var replayUUID = uuid.v4()
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
  testConnection(function(client, server) {
    var firstStoreUUID = uuid.v4()
    var secondStoreUUID = uuid.v4()
    var replayUUID = uuid.v4()
    var messageCount = 0
    var messages = [ ]
    client.on('data', function(data) {
      messageCount++
      messages.push(data)
      if (messageCount === 4) {
        console.log(JSON.stringify(messages, null, 2))
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

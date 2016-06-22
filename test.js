var tape = require('tape')
var testConnection = require('./test-connection')
var uuid = require('uuid')

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

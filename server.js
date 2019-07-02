#!/usr/bin/env node
var NAME = require('./package.json').name
var LEVELDOWN = process.env.LEVELDOWN || ('.' + NAME + '.leveldb')
var BLOBS = process.env.BLOBS || ('.' + NAME + '.blobs')
var PORT = parseInt(process.env.PORT) || 8089

var crypto = require('crypto')
var level = LEVELDOWN === 'memory'
<<<<<<< Updated upstream
 ? require('levelup')('tcp-log-server', {db: require('memdown')})
 : require('levelup')(LEVELDOWN, {db: require('leveldown')})
||||||| merged common ancestors
  ? require('levelup')('tcp-log-server', { db: require('memdown') })
  : require('levelup')(LEVELDOWN, { db: require('leveldown') })
=======
  ? require('levelup')(require('encoding-down')(require('memdown')()))
  : require('levelup')(require('leveldown')(LEVELDOWN))
>>>>>>> Stashed changes
var pino = require('pino')()
var handler = require('./')(
  pino,
  require('level-simple-log')(level),
  BLOBS === 'memory'
    ? require('abstract-blob-store')()
    : require('fs-blob-store')(BLOBS),
  new (require('events').EventEmitter)(),
  function (argument) {
    return crypto.createHash('sha256')
      .update(argument)
      .digest('hex')
  }
)

var sockets = require('stream-set')()
var server = require('net').createServer()
  .on('connection', function (socket) {
    sockets.add(socket)
  })
  .on('connection', handler)

server.listen(PORT, function () {
  pino.info({
    event: 'listening',
    port: this.address().port
  })
})

process.on('exit', function () {
  sockets.forEach(function (socket) {
    socket.destroy()
  })
  server.close()
})

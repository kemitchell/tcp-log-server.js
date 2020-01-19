#!/usr/bin/env node
var NAME = require('./package.json').name
var FILE = process.env.FILE || ('.' + NAME + '.file')
var BLOBS = process.env.BLOBS || ('.' + NAME + '.blobs')
var PORT = parseInt(process.env.PORT) || 8089

var crypto = require('crypto')
var pino = require('pino')()
var handler = require('./')(
  pino,
  FILE,
  BLOBS === 'memory'
    ? require('abstract-blob-store')()
    : require('fs-blob-store')(BLOBS),
  new (require('events').EventEmitter)(),
  function (argument) {
    return crypto.createHash('sha256')
      .update(argument)
      .digest('hex')
  },
  64
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

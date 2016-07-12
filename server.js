#!/usr/bin/env node
var NAME = require('./package.json').name
var LEVELDB = process.env.LEVELDB || ('.' + NAME + '.leveldb')
var BLOBS = process.env.BLOBS || ('.' + NAME + '.blobs')
var PORT = parseInt(process.env.PORT) || 8089

var level = require('levelup')(LEVELDB, {db: require('leveldown')})
var pino = require('pino')()
var handler = require('./')(
  pino,
  require('level-logs')(level, {valueEncoding: 'json'}),
  require('fs-blob-store')(BLOBS),
  new (require('events').EventEmitter)(),
  require('sha256')
)

var sockets = require('stream-set')()
var server = require('net').createServer()
.on('connection', function (socket) { sockets.add(socket) })
.on('connection', handler)

server.listen(PORT, function () {
  pino.info({event: 'listening', port: this.address().port})
})

process.on('exit', function () {
  sockets.forEach(function (socket) { socket.destroy() })
  server.close()
})

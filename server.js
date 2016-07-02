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
  new (require('events').EventEmitter)()
)

var server = require('net').createServer()
  .on('connection', handler)
  .once('close', function () { level.close() })

server.listen(PORT, function () {
  pino.info({event: 'listening', port: this.address().port})
})

process.on('exit', function () { server.close() })

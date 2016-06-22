#!/usr/bin/env node
var net = require('net')
var meta = require('./package.json')

var LEVELDB = ( process.env.LEVELDB || ( '.' + meta.name + '.leveldb' ) )
var level = require('level')(LEVELDB)
var logs =  require('level-logs')(level, { valueEncoding: 'json' })
var pino = require('pino')()
var BLOBS = ( process.env.BLOBS || ( '.' + meta.name + '.blobs' ) )
var blobs = require('fs-blob-store')(BLOBS)

var emitter = new (require('events').EventEmitter)()

var handler = require('./')(pino, logs, blobs, emitter)

var server = net.createServer()

server.on('connection', handler)

var PORT = ( parseInt(process.env.PORT) || 8089 )
server.listen(PORT, function() {
  pino.info({ event: 'listening', port: this.address().port }) })

process.on('exit', function() {
  server.close(function() {
    level.close() }) })

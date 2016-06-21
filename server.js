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

process.on('exit', function() {
  server.close(function() {
    level.close() }) })

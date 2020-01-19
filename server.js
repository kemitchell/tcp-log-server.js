#!/usr/bin/env node
const NAME = require('./package.json').name
const FILE = process.env.FILE || ('.' + NAME + '.file')
const BLOBS = process.env.BLOBS || ('.' + NAME + '.blobs')
const PORT = parseInt(process.env.PORT) || 8089

const pino = require('pino')()
const handler = require('./')({
  log: pino,
  file: FILE,
  blogs: BLOBS === 'memory'
    ? require('abstract-blob-store')()
    : require('fs-blob-store')(BLOBS),
  emitter: new (require('events').EventEmitter)()
})

const sockets = require('stream-set')()
const server = require('net').createServer()
  .on('connection', (socket) => { sockets.add(socket) })
  .on('connection', handler)

server.listen(PORT, () => {
  pino.info({
    event: 'listening',
    port: this.address().port
  })
})

process.on('exit', () => {
  sockets.forEach((socket) => { socket.destroy() })
  server.close()
})

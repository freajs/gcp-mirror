'use strict'

const { Storage } = require('@google-cloud/storage')
const miss = require('mississippi')
const crypto = require('crypto')
const once = require('once').strict
const got = require('got')
const { LoggingBunyan } = require('@google-cloud/logging-bunyan')
const bunyan = require('bunyan')

function initLogger (url, path, shasum) {
  const stackdriver = (new LoggingBunyan()).stream('info')
  const log = bunyan.createLogger({
    name: 'frea-packages',
    level: 'info',
    streams: [
      stackdriver
    ],
    url,
    path,
    shasum
  })

  log.callback = (cb) => () => stackdriver.stream.end(cb)

  return log
}

const storage = new Storage()
const bucket = storage.bucket('freajs')

exports.tarballs = function tarballs (message, _, cb) {
  const url = Buffer.from(message.data || '', 'base64').toString()
  const { path, shasum } = message.attributes
  const log = initLogger(url, path, shasum)
  const callback = once(log.callback(cb))
  log.info('processing')

  // If we weren't given a url, this message cant be handled
  // so discard it
  if (url.length === 0) {
    log.error('invalid message length')
    return callback()
  }

  const integrity = crypto.createHash('sha1')
  integrity.setEncoding('hex')
  const integrityCheck = miss.through(
    function integrityCheckHandler (chunk, enc, cb2) {
      const callback2 = once(cb2)
      integrity.update(chunk)
      return callback2(null, chunk)
    }
  )

  const file = bucket.file(path)
  miss.pipe(
    got.stream(url),
    integrityCheck,
    file.createWriteStream({
      contentType: 'application/gzip',
      public: true,
      resumable: false
    }),
    function (e) {
      if (e) {
        log.error({ err: e }, 'failed to download/upload')
        return callback()
      }

      integrity.end()
      const hash = integrity.read()

      if (hash === shasum) {
        return callback()
      }

      log.error({ hash }, 'failed integrity check')

      file.delete(callback)
    })
}

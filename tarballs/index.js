'use strict'

const { Storage } = require('@google-cloud/storage')
const storage = new Storage()
const logger = require('pino')({
  name: 'frea-tarballs',
  level: 'error'
})
const bucket = storage.bucket('freajs')
const miss = require('mississippi')
const crypto = require('crypto')
const once = require('once').strict
const got = require('got')

exports.tarballs = function tarballs (message, _, cb) {
  const callback = once(cb)
  const url = Buffer.from(message.data || '', 'base64').toString()
  const { path, shasum } = message.attributes

  const log = logger.child({
    url, path, shasum
  })

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
        log.error('failed to download/upload', { e })
        return callback()
      }

      integrity.end()
      const hash = integrity.read()

      if (hash === shasum) {
        return callback()
      }

      log.error('failed integrity check', {
        hash
      })

      file.delete(callback)
    })
}

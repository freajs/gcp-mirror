'use strict'

const { each, parallel } = require('async')
const Registry = require('npm-change-resolve')
const { PubSub } = require('@google-cloud/pubsub')
const once = require('once').strict
const path = require('path')
const { Storage } = require('@google-cloud/storage')
const str = require('string-to-stream')
const miss = require('mississippi')

const logger = require('pino')({
  name: 'frea-tarballs',
  level: 'error'
})

const pubsub = new PubSub()
const topic = pubsub.topic('tarballs')
topic.setPublishOptions({
  batching: {
    maxMessages: 0
  }
})

const registry = new Registry()

const storage = new Storage()
const bucket = storage.bucket('freajs')

exports.packages = function packages (message, _, cb) {
  const callback = once(cb)
  const data = Buffer.from(message.data || '', 'base64').toString()

  const log = logger.child({
    package: data
  })

  // If we weren't given a change.id, this message cant be handled
  // so discard it
  if (data.length === 0) {
    log.error('invalid message length')
    return callback()
  }

  registry.get(data, (e, manifest) => {
    if (e) {
      log.error('failed to fetch manifest', { e })
      return callback()
    }
    if (!manifest.versions) {
      log.error('Didnt resolve versions', { manifest })
      return callback()
    }
    if (!manifest.tarballs) {
      log.error('Didnt resolve versions', { manifest })
      return callback()
    }

    parallel([
      (cb2) => each(
        manifest.tarballs,
        (tarball, cb3) => handleTarball({ log }, tarball, cb3),
        cb2),
      (cb2) => each(
        manifest.versions,
        (version, cb3) => handleVersion({ log }, version, cb3)
        , cb2),
      (cb2) => uploadIndex({ log }, manifest, cb2)
    ], callback)
  })
}

function handleTarball (opts, tarball, cb) {
  const log = opts.log
  const callback = once(cb)
  if (!tarball.path) {
    log.error('tarball did not include path', { tarball })
    return callback()
  }
  if (!tarball.shasum) {
    log.error('tarball did not include shasum', { tarball })
    return callback()
  }
  if (!tarball.tarball) {
    log.error('tarball did not include url', { tarball })
    return callback()
  }
  const msgAttributes = {
    path: String(tarball.path),
    shasum: String(tarball.shasum)
  }
  const url = Buffer.from(String(tarball.tarball))
  topic.publish(url, msgAttributes, (e, msgId) => {
    if (e) {
      log.error('failed to publish message', {
        e,
        url,
        msgAttributes
      })
    }
    callback()
  })
}

function handleVersion (opts, version, cb) {
  const log = opts.log
  const callback = once(cb)
  if (!version.json || !version.json.name) {
    log.error('version did not include json.name', { version })
    return callback()
  }
  if (!version.version) {
    log.error('version did not include version', { version })
    return callback()
  }
  const filename = path.join(
    version.json.name,
    version.version,
    'index.json')
  const file = bucket.file(filename)
  miss.pipe(
    str(JSON.stringify(version.json, null, '    ')),
    file.createWriteStream({
      contentType: 'application/json',
      public: true,
      resumable: false
    }),
    function (e) {
      log.error('failed to download/upload', {
        name: version.json.name,
        version: version.version,
        e
      })
      callback()
    }
  )
}

function uploadIndex (opts, index, cb) {
  const log = opts.log
  const callback = once(cb)
  if (!index.json || !index.json.name) {
    log.error('index did not include json.name', { index })
    return callback()
  }
  const filename = path.join(
    index.json.name,
    'index.json')
  const file = bucket.file(filename)
  miss.pipe(
    str(JSON.stringify(index.json, null, '    ')),
    file.createWriteStream({
      contentType: 'application/json',
      public: true,
      resumable: false
    }),
    function (e) {
      log.error('failed to download/upload', { e })
      callback()
    })
}

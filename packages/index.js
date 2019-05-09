'use strict'

const { each, parallel } = require('async')
const Registry = require('npm-change-resolve')
const { PubSub } = require('@google-cloud/pubsub')
const once = require('once').strict
const path = require('path')
const { Storage } = require('@google-cloud/storage')
const str = require('string-to-stream')
const miss = require('mississippi')
const { LoggingBunyan } = require('@google-cloud/logging-bunyan')
const bunyan = require('bunyan')

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

function initLogger (data) {
  const stackdriver = (new LoggingBunyan()).stream('info')
  const log = bunyan.createLogger({
    name: 'frea-packages',
    level: 'info',
    streams: [
      stackdriver
    ],
    package: data
  })

  log.callback = (cb) => () => stackdriver.stream.end(cb)

  return log
}

exports.packages = function packages (message, _, cb) {
  const data = Buffer.from(message.data || '', 'base64').toString()
  const log = initLogger(data)
  const callback = once(log.callback(cb))
  log.info('processing')

  // If we weren't given a change.id, this message cant be handled
  // so discard it
  if (data.length === 0) {
    log.error('invalid message length')
    return callback()
  }

  registry.get(data, (e, manifest) => {
    if (e) {
      log.error({ err: e }, 'failed to fetch manifest')
      return callback()
    }
    if (!manifest.versions) {
      log.error({ manifest }, 'didnt resolve versions')
      return callback()
    }
    if (!manifest.tarballs) {
      log.error({ manifest }, 'didnt resolve tarballs')
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
    log.error({ tarball }, 'tarball did not include path')
    return callback()
  }
  if (!tarball.shasum) {
    log.error({ tarball }, 'tarball did not include shasum')
    return callback()
  }
  if (!tarball.tarball) {
    log.error({ tarball }, 'tarball did not include url', { tarball })
    return callback()
  }
  const msgAttributes = {
    path: String(tarball.path),
    shasum: String(tarball.shasum)
  }
  const url = Buffer.from(String(tarball.tarball))
  topic.publish(url, msgAttributes, (e, msgId) => {
    if (e) {
      log.error({ e, url, msgAttributes }, 'failed to publish message')
    }
    return callback()
  })
}

function handleVersion (opts, version, cb) {
  const log = opts.log
  const callback = once(cb)
  if (!version.json || !version.json.name) {
    log.error({ version }, 'version did not include json.name')
    return callback()
  }
  if (!version.version) {
    log.error({ version }, 'version did not include version')
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
      if (e) {
        log.error({
          packageName: version.json.name,
          packageVersion: version.version,
          e
        }, 'failed to upload manifest')
      }
      callback()
    }
  )
}

function uploadIndex (opts, index, cb) {
  const log = opts.log
  const callback = once(cb)
  if (!index.json || !index.json.name) {
    log.error({ index }, 'index did not include json.name')
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
      if (e) {
        log.error({ err: e }, 'failed to upload index')
      }
      callback()
    })
}

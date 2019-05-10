'use strict'

// Load all dependencies
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

// Configure Google Cloud Pub/Sub
const pubsub = new PubSub()
const topic = pubsub.topic('tarballs')
topic.setPublishOptions({
  // Dont batch messages! We want to flush to Pub/Sub immediately since this
  // is a Cloud Function invocation
  batching: {
    maxMessages: 0
  }
})

// Configure Google Cloud Storage
const storage = new Storage()
const bucket = storage.bucket('freajs')

// Configure our registry resolver
// This takes a package name and resolves it to:
//   * A package level manifest
//   * A set of manifests for each published version
//   * A list of urls to the tarballs containing each published version
const registry = new Registry()

// initLogger creates a dedicated logger for each Cloud Function invocation.
// Since logging is async, there isn't a guarentee by default that logs will
// be fully flushed when the the Cloud Function terminates, meaning logs may not
// make it to stackdriver! Since we use the stackdriver logs for retrying
// failed downloads, this is unacceptable!
// By having a dedicated stackdriver stream for every invocation, we can
// force the stream to fully flush before inovking the function's callback.
// This returns a standard bunyan logger with an extra function: callback.
// Callback wraps the Cloud Function's default callback w/ logic that forces
// the stream to be fully flushed prior to terminating the function. Magic!
function initLogger (data) {
  // Create a new stackdriver stream dedicated to this invocation of the
  // cloud function
  const stackdriver = (new LoggingBunyan()).stream('info')
  // Register the stream as a bunyan target
  const log = bunyan.createLogger({
    name: 'frea-packages',
    level: 'info',
    streams: [
      stackdriver
    ],
    // Include the package name this Cloud Function invocation is handling in
    // every log
    package: data
  })

  // Create a callback function that wraps the Cloud Function's callback
  // with logic that closes the stackdriver stream and waits for it to fully
  // flush before invoking the Cloud Function's callback. This guarentees the
  // logs will be written to stackdriver before the function terminates.
  log.callback = (cb) => () => stackdriver.stream.end(cb)

  // Return our new bunyan instance
  return log
}

// This is our Cloud Function handler, it can be deployed with:
// gcloud functions deploy packages --runtime nodejs8 --trigger-topic packages
// This Cloud Function is quite large thanks to error handling logic, but what
// it does is rather straightforward:
// This Cloud Function accepts a npmjs package name and uses npm-change-resolve
// to get all the information necessary to mirror the package, it then uses that
// information to simultaneously:
// 1. Trigger the tarballs Cloud Function for each individual tarball that needs
//    to be mirrored by publishing it's url, shasum, and Google Cloud Storage
//    path to the tarballs Cloud Pub/Sub topic
// 2. Uploads the package level manifest to Google Cloud Storage
// 3. Uploads the individual manifest for each published version of the package
//    to Google Cloud Storage
// When this finishes it's invocation, all of the manifests for a package will
// be being mirrored, and we should have triggered a set of Cloud Functions that
// will be downloading the tarballs. Once the Cloud Functions finish downloading
// the tarballs the entire package will be mirrored.
exports.packages = function packages (message, _, cb) {
  // Parse the package name from the base64 encoded Pub/Sub message
  const data = Buffer.from(message.data || '', 'base64').toString()
  // Create a dedicated logger for this Cloud Function invocation, all of the
  // logs from this invocation will include the package name we are handling
  const log = initLogger(data)
  // Create a callback for this Cloud Function invocation that ensures all logs
  // are written to stackdriver before terminating. We also wrap it in once,
  // which will throw an exception if we try to call this more than once. Once
  // is handy for catching async logic bugs at runtime.
  const callback = once(log.callback(cb))
  // Write out an informational log that let's us know what package this
  // invocation is handling
  log.info('processing')

  // If we weren't given a change.id, this message cant be handled so discard
  // it. This should never happen, but it's probably wise to guard against.
  if (data.length === 0) {
    log.error('invalid message length')
    return callback()
  }

  // Retrieve all the information we need to mirror the registry
  registry.get(data, (e, manifest) => {
    // If we failed to fetch the information we need to mirror the registry,
    // we can't continue
    if (e) {
      log.error({ err: e }, 'failed to fetch manifest')
      return callback()
    }
    // The next five blocks validate the form of the data we got back from
    // npm-change-resolve, this protects against exceptions being thrown.
    if (!manifest.json) {
      log.error({ manifest }, 'didnt resolve package level manifest')
      return callback()
    }
    if (!manifest.versions) {
      log.error({ manifest }, 'didnt resolve versions')
      return callback()
    }
    if (!Array.isArray(manifest.versions)) {
      log.error({ manifest }, 'manifest.versions must be an array')
      return callback()
    }
    if (!manifest.tarballs) {
      log.error({ manifest }, 'didnt resolve tarballs')
      return callback()
    }
    if (!Array.isArray(manifest.tarballs)) {
      log.error({ manifest }, 'manifest.tarballs must be an array')
      return callback()
    }

    // So this is a bit of a bear, but is a really effective way at downloading
    // all of this stuff _really_ fast.
    // parallel is running each of the three listed functions at the same time.
    parallel([
      // The first function will publish each tarball url to the tarballs
      // Google Cloud Pub/Sub service at the same time. This allows us to
      // horizontally scale the tarball download/upload, which is network and
      // CPU intensive (shasum is expensive to compute!).
      (cb2) => each(
        manifest.tarballs,
        (tarball, cb3) => handleTarball({ log }, tarball, cb3),
        cb2),
      // The second function uploads the manifest for every published version of
      // this package to Google Cloud Storage at the same time, since we already
      // have everything we need in memory there isn't much value in
      // horizontally scaling this, so we just upload directly from this
      // function
      (cb2) => each(
        manifest.versions,
        (version, cb3) => handleVersion({ log }, version, cb3)
        , cb2),
      // The third function uploads the package level manifest to Google Cloud
      // storage
      (cb2) => uploadIndex({ log }, manifest.json, cb2)
      // Once everything is done, we wait for the stackdriver logs to flush and
      // then end execution of the Cloud Function
    ], callback)
  })
}

// handleTarball will publish everything necessary to download a package's
// tarball from npm and upload it to Google Cloud Pub/Sub, validating the
// integrity of the package along the way.
function handleTarball (opts, tarball, cb) {
  const log = opts.log
  // Wrap the callback in a once handler to catch any logic bugs at runtime
  const callback = once(cb)

  // The next three blocks ensure the tarball definition contains everything we
  // need to mirror the package
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

  // There are three things we need to download a version of a package:
  //   - The path we should upload the artifact to in Google Cloud Storage
  //   - The shasum of the tarball so we can validate the right bits came
  //     across the wire (we don't want to mirror a corrupted file!)
  //   - The url that we need for downloading the tarball from npm
  // Since there are three things, we store two as attributes and one as the
  // message when publishing to Google Cloud Pub/Sub
  const msgAttributes = {
    path: String(tarball.path),
    shasum: String(tarball.shasum)
  }
  const url = Buffer.from(String(tarball.tarball))
  // Send it over to the tarballs Cloud Function
  topic.publish(url, msgAttributes, (e, msgId) => {
    // If we failed to publish the message to the topic, log an error so we
    // can retry later
    if (e) {
      log.error({ err: e, url, msgAttributes }, 'failed to publish message')
    }
    // All done with this tarball! At this point it is in the tarballs topic in
    // Google Cloud Pub/Sub, which will be read by the tarballs Cloud Function.
    return callback()
  })
}

// handleVersion uploads a package version's manifest to Google Cloud Storage
function handleVersion (opts, version, cb) {
  const log = opts.log
  // Wrap the callback w/ once to catch logic errors at runtime
  const callback = once(cb)

  // The next two blocks validate the form of the manifest before uploading
  if (!version.json || !version.json.name) {
    log.error({ version }, 'version did not include json.name')
    return callback()
  }
  if (!version.version) {
    log.error({ version }, 'version did not include version')
    return callback()
  }

  // Construct the path we are uploading the manifest to, this keeps URL parity
  // with npm so that npm clients can use it.
  const filename = path.join(
    version.json.name,
    version.version,
    'index.json')

  // Create the Google Cloud Storage target for uploading to
  const file = bucket.file(filename)

  // Mississippi is a handy tool for managing Node.js streams
  miss.pipe(
    // Create a steam out of the string representation of the manifest
    str(JSON.stringify(version.json, null, '    ')),
    // Stream the string to Google Cloud Storage
    file.createWriteStream({
      contentType: 'application/json',
      public: true,
      resumable: false
    }),
    function (e) {
      // If the upload failed, log a message to stackdriver so we can retry
      // later
      if (e) {
        log.error({
          packageName: version.json.name,
          packageVersion: version.version,
          err: e
        }, 'failed to upload manifest')
      }
      // And we are done! At this point the manifest for this version of the
      // package is being mirrored!
      callback()
    }
  )
}

// uploadIndex uploads a package's manifest to Google Cloud Storage
function uploadIndex (opts, manifest, cb) {
  const log = opts.log
  // Wrap the callback in once to catch logic errors at runtime
  const callback = once(cb)

  // Validate the form of the manifest, well at least the pieces we need for
  // uploading
  if (!manifest || !manifest.name) {
    log.error({ manifest }, 'manifest did not include a name')
    return callback()
  }

  // Construct the path we are uploading the manifest to, this keeps URL parity
  // with npm so that npm clients can use it.
  const filename = path.join(
    manifest.name,
    'index.json')

  // Create the Google Cloud Storage target for uploading to
  const file = bucket.file(filename)

  // Mississippi is a handy tool for managing Node.js streams
  miss.pipe(
    // Create a steam out of the string representation of the manifest
    str(JSON.stringify(manifest, null, '    ')),
    // Stream the string to Google Cloud Storage
    file.createWriteStream({
      contentType: 'application/json',
      public: true,
      resumable: false
    }),
    function (e) {
      // If the upload failed, log a message to stackdriver so we can retry
      // later
      if (e) {
        log.error({ err: e }, 'failed to upload index')
      }
      // And we are done! At this point the manifest for the package is
      // being mirrored!
      callback()
    })
}

'use strict'

// Load all dependencies
const { Storage } = require('@google-cloud/storage')
const miss = require('mississippi')
const crypto = require('crypto')
const once = require('once').strict
const got = require('got')
const { LoggingBunyan } = require('@google-cloud/logging-bunyan')
const bunyan = require('bunyan')

// Configure Google Cloud Storage
const storage = new Storage()
const bucket = storage.bucket('freajs')

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
function initLogger (url, path, shasum) {
  // Create a new stackdriver stream dedicated to this invocation of the
  // cloud function
  const stackdriver = (new LoggingBunyan()).stream('info')
  const log = bunyan.createLogger({
    name: 'frea-packages',
    level: 'info',
    streams: [
      stackdriver
    ],
    // Include the url, path, and shasum for the tarball this Cloud Function
    // invocation is handling in every log
    url,
    path,
    shasum
  })

  // Create a callback function that wraps the Cloud Function's callback
  // with logic that closes the stackdriver stream and waits for it to fully
  // flush before invoking the Cloud Function's callback. This guarentees the
  // logs will be written to stackdriver before the function terminates.
  log.callback = (cb) => () => stackdriver.stream.end(cb)

  // Return our new bunyan instance
  return log
}

// This is our Cloud Function handler for download a package version's tarball.
// This function get's invoked by the packages Cloud Function. There isn't
// much to this function, it downloads a single tarball from the npm registry
// and uploads it to Google Cloud storage. Once it is done executing, this
// tarball will be fully mirrored.
exports.tarballs = function tarballs (message, _, cb) {
  // Parse the tarball's url from the base64 encoded Pub/Sub message
  const url = Buffer.from(message.data || '', 'base64').toString()
  // Get the path and shasum from the message attributes
  const { path, shasum } = message.attributes
  // Create a dedicated logger for this Cloud Function invocation, all of the
  // logs from this invocation will include the url, path, and shasum of the
  // tarball we are handling
  const log = initLogger(url, path, shasum)
  // Create a callback for this Cloud Function invocation that ensures all logs
  // are written to stackdriver before terminating. We also wrap it in once,
  // which will throw an exception if we try to call this more than once. Once
  // is handy for catching async logic bugs at runtime.
  const callback = once(log.callback(cb))
  // Write out an informational log that let's us know what tarball this
  // invocation is handling
  log.info('processing')

  // If we weren't given a url, this message cant be handled so discard it.
  // This should never happen but its probably wise to guard against.
  if (url.length === 0) {
    log.error('invalid message length')
    return callback()
  }

  // Create a stream based shasum hasher. This will generate the shasum for the
  // package on the fly while we download/upload the tarball to Google Cloud
  // Storage. This allows us to compute the shasum without having to buffer
  // the entire package into memory.
  const integrity = crypto.createHash('sha1')
  integrity.setEncoding('hex')
  const integrityCheck = miss.through(
    function integrityCheckHandler (chunk, enc, cb2) {
      const callback2 = once(cb2)
      integrity.update(chunk)
      return callback2(null, chunk)
    }
  )

  // Create the Google Cloud Storage target for uploading to
  const file = bucket.file(path)

  // Mississippi is a handy tool for managing Node.js streams
  miss.pipe(
    // Download the tarball from npm
    got.stream(url),
    // Compute the shasum so we can validate the integrity of the file after
    // upload
    integrityCheck,
    // Upload the file to Google Cloud Storage
    file.createWriteStream({
      contentType: 'application/gzip',
      public: true,
      resumable: false
    }),
    function (e) {
      // If the upload/download failed, log the error to stackdriver so we can
      // retry later
      if (e) {
        log.error({ err: e }, 'failed to download/upload')
        return callback()
      }

      // Validate the integrity of the file we downloaded from the registry,
      // this ensure we received the correct bits over the wire and that the
      // file wasn't corrupted during download. Note: we don't need to validate
      // the upload, the Google Cloud Storage library uses crc32c to validate
      // the upload for us.
      integrity.end()
      const hash = integrity.read()
      if (hash === shasum) {
        // If the shasum matched, we are done! The tarball is now being
        // mirrored!
        return callback()
      }

      // If the shasum didn't match, the file was corrupted during download.
      // Log an error to stackdriver so we can try again later and then delete
      // the file from Cloud Storage.
      log.error({ hash }, 'failed integrity check')
      file.delete(callback)
    })
}

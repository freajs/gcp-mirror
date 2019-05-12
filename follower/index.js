'use strict'

// Load all dependencies
const { LoggingBunyan } = require('@google-cloud/logging-bunyan')
const { PubSub } = require('@google-cloud/pubsub')
const Firestore = require('@google-cloud/firestore')
const bunyan = require('bunyan')
const follow = require('follow')
const rateLimit = require('function-rate-limit')

// Configure Cloud Pub/Sub
const pubsub = new PubSub()
const topic = pubsub.topic('packages')

// Configure Logging
const log = bunyan.createLogger({
  name: 'frea-follower',
  level: 'info',
  streams: [
    (new LoggingBunyan()).stream('info')
  ]
})

let seq = 0
// Setup persisting the Sequence ID from Cloud Firestore
const db = new Firestore()
const doc = db.collection('follower').doc('replicate.npmjs.com')

// Track the current sequence number so we can resume if the process crashes

// Fetch the current persisted sequence number from Cloud Firestore
doc.get()
  .then((doc) => {
    // This should never happen but probably wise to protect against
    if (!doc.exists) {
      // console.error is sync so we get guarenteed logs using it
      console.error('unable to get seq')
      process.exit(1)
    }
    // Update the sequence number from the doc
    seq = doc.data().seq
    log.info(doc, 'starting follower')

    // Start following the npm registry
    startFollowing()
  })
  .catch((e) => {
    // If we failed to get the value there is nothing left to do
    // console.error is sync so we get guarenteed logs using it
    console.error('unable to get seq')
    console.error(e)
    process.exit(1)
  })

// Keep track of whether
let syncingSeq = false
function syncSeq () {
  if (syncingSeq) {
    return
  }
  syncingSeq = true
  doc.set({ seq })
    .then(() => {
      syncingSeq = false
    })
    .catch((e) => {
      syncingSeq = false
      log.error({ err: e }, 'failed to sync seq')
    })
}

setInterval(syncSeq, 5000)

function startFollowing () {
  follow({
    db: 'https://replicate.npmjs.com/registry',
    since: seq,
    inactivity_ms: 3600000
  }, rateLimit(12, 1000, handleChange))
}

function handleChange (e, change) {
  if (e) {
    log.error({ err: e }, 'failed to get change')
  }

  if (!change || !change.id) {
    log.error({ change }, 'change did not include an id')
    return
  }

  log.info({ change }, 'publishing')

  const changeId = Buffer.from(String(change.id))

  topic.publish(changeId, function topicPublished (e) {
    if (change.seq > seq) {
      seq = change.seq
    }

    log.info({ change }, 'published')

    if (e) {
      log.error({ err: e }, 'failed to publish topic')
    }
  })
}

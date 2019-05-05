'use strict'

const { PubSub } = require('@google-cloud/pubsub')
const follow = require('follow')
const pino = require('pino')

const pubsub = new PubSub()
const topic = pubsub.topic('packages')
topic.setPublishOptions()

const log = pino({
  name: 'frea-follower',
  level: 'error'
})

follow({
  db: 'https://replicate.npmjs.com/registry',
  since: 1000,
  inactivity_ms: 3600000
}, handleChange)

function handleChange (e, change) {
  if (e) {
    log.error('failed to get change', { e })
  }

  if (!change || !change.id) {
    return
  }

  topic.publish(String(change.id), (e) => {
    if (e) {
      log.error('failed to publish topic', { e })
    }
  })
}

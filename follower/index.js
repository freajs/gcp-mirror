'use strict'

// const { PubSub } = require('@google-cloud/pubsub')

const follow = require('follow')
const queue = require('async').queue

follow({
  db: 'https://replicate.npmjs.com/registry',
  since: 1000,
  inactivity_ms: 3600000
}, handleChange)

function handleChange (e, change) {
  if (change === undefined || change.seq === undefined) {
    return
  }
  console.log(change.id)
}

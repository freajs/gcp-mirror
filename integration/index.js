const proxyquire = require('proxyquire')
const PubSubStub = require('./pubsub.stub.js')
const StorageStub = require('./storage.stub.js')
const LoggingBunyanStub = require('./logging-bunyan.stub.js')
const rateLimit = require('function-rate-limit')
const stubs = {
  '@google-cloud/pubsub': PubSubStub,
  '@google-cloud/storage': StorageStub,
  '@google-cloud/logging-bunyan': LoggingBunyanStub
}

const { tarballs } = proxyquire('../tarballs/index.js', stubs)
const { packages } = proxyquire('../packages/index.js', stubs)

const { PubSub } = PubSubStub
const pubsub = new PubSub()
const packagesTopic = pubsub.topic('packages')
const tarballsTopic = pubsub.topic('tarballs')
packagesTopic.register(rateLimit(1, 2000, packages))
tarballsTopic.register(rateLimit(5, 1000, tarballs))

proxyquire('../follower/index.js', stubs)

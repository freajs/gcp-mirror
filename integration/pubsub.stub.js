class Topic {
  constructor() {
    this.listeners = []
  }
  register(func) {
    this.listeners.push(func)
  }
  publish(data, attributes, cb) {
    if(typeof attributes === 'function') {
      cb = attributes
      attributes = {}
    }
    if(!cb) {
      cb = function(){}
    }
    const message = {
      data: data.toString('base64'),
      attributes
    }
    this.listeners.forEach((v) => v(message, {}, cb))
  }
  setPublishOptions() {}
}

topics = {}

class PubSub {
  topic(topic) {
    topics[topic] = topics[topic] || new Topic()
    return topics[topic]
  }
}

module.exports = { PubSub }

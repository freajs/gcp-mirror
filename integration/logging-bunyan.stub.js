const { Writable } = require('stream')

class LogStream extends Writable {
  write(chunk) {
    process.stdout.write(chunk)
  }
}

class LoggingBunyan {
  stream(level) {
    const stream = new LogStream()
    return {
      stream,
      level
    }
  }
}

module.exports = { LoggingBunyan }

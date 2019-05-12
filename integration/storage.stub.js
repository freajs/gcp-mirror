const mkdirp = require('mkdirp')
const fs = require('fs-blob-store')
const path = require('path')

class File {
  constructor(bucket, name) {
    this.bucket = bucket
    this.name = name
  }
  createWriteStream() {
    return this.bucket.createWriteStream({ key: this.name })
  }
  delete(cb) {
    this.bucket.remove({ key: this.name }, cb)
  }
}

class Bucket {
  constructor(name) {
    this.name = path.join('.', name)
    mkdirp.sync(this.name)
    this.bucket = fs(this.name)
  }
  file(name) {
    return new File(this.bucket, name)
  }
}

class Storage {
  bucket(name) {
    return new Bucket(name)
  }
}

module.exports = { Storage }

'use strict';

const process = require('process')
const path = require('path')
const bunyan = require('bunyan')


const logger = bunyan.createLogger({
  level: 'info',
  streams: [
    {stream: process.stderr, level: 'info'}
  ],
  name: 'databack',
  serializers: bunyan.stdSerializers
})

/**
 @return a child logger for specified module
 */
logger.constructor.prototype.ofModule = function (m) {
  const NMP = path.sep + 'node_modules' + path.sep
  var mStr = m.filename || m.id
  var p = mStr.lastIndexOf(NMP)
  if (p >= 0) {
    mStr = mStr.substr(p + NMP.length)
  }
  p = mStr.indexOf(path.sep)
  if (p > 0) {
    mStr = mStr.substr(0, p) + ':' + mStr.substr(p + 1)
  }
  if (mStr) {
    return this.child({module: mStr})
  }

  return this.child({module: m.id})
}


module.exports = logger

'use strict';

const packageLogger = require('./log')
const logger = packageLogger.ofModule(module)


function opDelete(id) {
  return JSON.stringify({$op$: '$del$', $id$: id})
}


function serialize(doc) {
  return JSON.stringify(doc, function (prop, value) {

    // for nested data objects
    var realVal = this[prop]
    if (!Object.is(value, realVal)) {
      // the value has been altered, e.g. by custom toJSON() method
      if (realVal instanceof Date) {            // TODO more robust check, regarding envs like NW etc.
        return {$type$: 'date', time: realVal.getTime()}
      }
      // handle more situations here
    }

    return value
  })
}


function deserialize(ds, line) {
  var result = {}
  result.data = JSON.parse(line, function (prop, value) {

    // check if on top most object
    if ('' === prop) {
      // strip $id$ and save to local var
      result.id = value.$id$
      delete value.$id$

      // check if this line is an operation instead of doc data
      var op = value.$op$
      if (op) {
        result.op = op
      }
      return value
    }

    // this is nested data object
    if (typeof value === 'object') {
      var type = value.$type$
      if (!type) {
        // plain data object
        return value
      }
      // synthesized data object
      switch (type) {
        case 'date':
          return new Date(value.time)
          break
        default:
          let err = new Error('Unsupported persistence object $type$')
          logger.error({err, obj: value})
          if (!ds.emit('error', err)) {
            throw err
          }
      }
    }
    return value

  })
  return result
}


module.exports.opDelete = opDelete
module.exports.serialize = serialize
module.exports.deserialize = deserialize


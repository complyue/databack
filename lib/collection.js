'use strict';

const fs = require('fs')
const path = require('path')
const EventEmitter = require('events')
const crypto = require('crypto')

const packageLogger = require('./log')
const logger = packageLogger.ofModule(module)

const odm = require('./odm')

const Document = require('./document')
const Persis = require('./persis')


/**
 *
 * When we don't want client code to use some of our object properties,
 * we discourage that by using symbol indices for such properties. In this
 * regard, we don't technically close the road more aggressively like using
 * Object's freeze/seal/preventExtensions mechanism. Client code can still
 * enumerate the symbol keys and use them by checking their toString() values.
 *
 * We can further make it even harder to not name the symbols here, then the
 * client will have to guess by the property value.
 *
 */

// for Index
const $cmpr$ = Symbol('$cmpr$')
const $keyer$ = Symbol('$keyer$')
const $tree$ = Symbol('$tree$')
const $keys$ = Symbol('$keys$')
const $add$ = Symbol('$add$')
const $upd$ = Symbol('$upd$')
const $del$ = Symbol('$del$')
const $reset$ = Symbol('$reset$')

// for Collection
const $persis$ = Symbol('$persis$')


function defaultIdGen() {

  function d2(i) {
    if (i < 10) return '0' + i
    else return '' + i
  }

  var d = new Date()
  return d.getFullYear() + d2(d.getMonth()) + d2(d.getDate())
    + '~' + String(Date.now()).substr(-8) + '~'
    + crypto.randomBytes(4).toString('base64').replace(/[+\/]/g, '_').substr(0, 5)
}


class Collection extends EventEmitter {

  constructor(filename, {indices={}, onload=null, compactOnload=true, idGen=defaultIdGen} = {}) {
    super()
    this.allDocs = new Map()
    this.indices = {}
    this.idGen = idGen
    if (onload) {
      this.once('load', onload)
    }

    if (!filename) {
      // not persistent, initialize as empty ds
      this.addIndices(indices)
      this.emit('load')
      return
    }

    // is a persistent datastore, load now
    filename = path.resolve(filename)
    this.filename = filename
    this[$persis$] = new Persis(this.filename, this)
    fs.readFile(filename, 'utf-8', (err, data) => {
      if (err) {
        if (err.code === 'ENOENT') {
          // file not exists, treat as empty
          logger.debug({err, filename}, 'persistent file does not exist')
          this.addIndices(indices)
          this.emit('load')
          return
        }

        if (!this.emit('error', err)) {
          throw err
        }
        logger.error({err}, 'failed reading persistent file')
        return
      }

      // load from file data
      var alreadyCompat = true
      try {
        var lines = data.split('\n')
        for (var line of lines) {
          line = line.trim()
          if (!line) continue
          var docData = odm.deserialize(this, line)
          logger.trace({docData}, 'persistent file line parsed')
          if (docData.op) {
            //  operations exist in the persistent file
            alreadyCompat = false

            switch (docData.op) {
              case '$del$':
                this.allDocs.delete(docData.id)
                break
              default:
                let err = new Error('Unsupported persistent op ' + docData.op)
                logger.error({err, docData})
                if (!this.emit('error', err)) {
                  throw err
                }
            }
          } else {
            if (this.allDocs.has(docData.id)) {
              // updates exist in the persistent file
              alreadyCompat = false
            }
            var doc = new Document(this, docData.id)
            Object.assign(doc, docData.data)
            this.allDocs.set(docData.id, doc)
          }
        }
      } catch (err) {
        logger.error({err, collection: this})
        if (!this.emit('error', err)) throw err
      }

      // initialize with loaded data
      this.addIndices(indices)
      this.emit('load')

      // data loaded, compact if requested
      if (compactOnload) {
        logger.debug({alreadyCompat}, 'compact on load')
        if (!alreadyCompat) {
          this.compact()
        }
      }
    })
  }

  /**
   * add indices to this collection
   *
   * @param indices
   */
  addIndices(indices) {
    var idx2load = []
    var idx
    for (var idxName in indices) {
      idx = new Index(idxName, indices[idxName])
      this.indices[idxName] = idx
      idx2load.push(idx)
    }
    for (var [, doc] of this.allDocs) {
      for (idx of idx2load) {
        idx[$add$](doc)
      }
    }
  }

  /**
   *
   * create new docs in this collection
   *
   * @param docDataList
   * @param cb
   * @returns {Array}
   */
  add(docDataList, cb) {
    if (!Array.isArray(docDataList)) {
      docDataList = [docDataList]
    }
    var docs = []
    for (var docData of docDataList) {
      var id = this.idGen()
      while (this.allDocs.has(id)) {
        logger.debug({idGen: this.idGen, id}, 'duplicate id generated')
        id = this.idGen()
      }
      var doc = new Document(this, id)
      Object.assign(doc, docData)
      this.allDocs.set(id, doc)
      for (var idxName in this.indices) {
        var idx = this.indices[idxName]
        idx[$add$](doc)
      }
      docs.push(doc)
    }
    this.save(docs, cb)
    return Array.isArray(arguments[0]) ? docs : docs[0]
  }

  /**
   * save updates on docs in this collection
   */
  save(docs, cb) {
    if (!Array.isArray(docs)) {
      docs = [docs]
    }
    var payload = []
    for (var doc of docs) {
      // update indices
      for (var idxName in this.indices) {
        var idx = this.indices[idxName]
        idx[$upd$](doc)
      }
      // serialize doc to a data line
      payload.push(odm.serialize(doc))
    }
    this[$persis$].queue(payload.join('\n') + '\n', (err)=> {
      if (err) {
        if (cb) {
          cb.call(this, err, docs)
        }
        if (!this.emit('error', err)) throw err
      } else {
        if (cb) {
          cb.call(this, null, docs)
        }
      }
    })
  }

  /**
   * compact persistent storage backing this collection
   */
  compact(cb) {

    var preceder

    const handleErr = (err)=> {
      if (!err)
        return true

      logger.errror({err})

      // resume normal persis, and backlog should retain as not successfully persisted by the compact
      this[$persis$].resume(preceder, false)

      // notify callback with error
      if (cb) {
        cb.call(this, err)
      }

      // emit error event, if not listened, throw it
      if (!this.emit('error', err)) {
        throw err
      }
      return true
    }

    // put normal persis to standby mode, after all in-progress writes finish, it retain
    // writes in current queue as backlog, and calls the callback here, further writes will be queued
    // to normal queue, but pending writing until resumed
    preceder = (err)=> {
      if (!handleErr(err)) return

      // take the snapshot data of all docs at this point
      var payload = []
      for (var [, doc] of this.allDocs) {
        // serialize doc to a data line
        payload.push(odm.serialize(doc))
      }

      var newFilename = this.filename + '~'
      var persis = new Persis(newFilename)
      persis.queue(payload.join('\n') + '\n', (err)=> {
          if (!handleErr(err)) return
          // crash-safe file saved, start renaming
          var toBeDelFilename = this.filename + '~del~'
          fs.unlink(toBeDelFilename, (err)=> {
            // ignore err for file not exists
            if (err.code !== 'ENOENT') {
              if (!handleErr(err)) return
            }
            fs.rename(this.filename, toBeDelFilename, (err)=> {
              if (!handleErr(err)) return
              fs.rename(newFilename, this.filename, (err)=> {
                if (!handleErr(err)) return
                fs.unlink(toBeDelFilename, (err)=> {
                  if (err) {
                    logger.warn({
                      err,
                      filename: this.filename,
                      renamedFileName: toBeDelFilename
                    }, 'can not delete the old version of persistent file')
                  }
                  // compact done successfully
                  // resume normal persis, while backlog should be discarded as those writes in it
                  // are also in the snapshot took above, thus already been persisted by the compact
                  this[$persis$].resume(preceder, true)
                  // notify callback and listeners
                  if (cb) {
                    cb.call(this, null)
                  }
                  this.emit('compact')
                })
              })
            })
          })
        }, true // sync when compact, or data loss may occur since old data file is deleted
      )

    }
    this[$persis$].standby(preceder)

  }

  /**
   * delete docs from this collection
   *
   * @param docs
   * @param cb
   */
  delete(docs, cb) {
    if (!Array.isArray(docs)) {
      docs = [docs]
    }
    var ids = []
    for (var doc of docs) {
      this.allDocs.delete(doc.$id$)
      for (var idxName in this.indices) {
        var idx = this.indices[idxName]
        idx[$del$](doc)
      }
      ids.push(doc.$id$)
    }
    var payload = []
    for (var id of ids) {
      payload.push(odm.opDelete(id))
    }
    this[$persis$].queue(payload.join('\n') + '\n', (err) => {
      if (err) {
        if (cb) {
          cb.call(this, err, ids)
        }
        if (!this.emit('error', err)) throw err
      } else {
        if (cb) {
          cb.call(this, null, ids)
        }
      }
    })
  }

}


const BST = require('binary-search-tree')
const SearchTree = BST.AVLTree

const ReadOnlyPD = require('./props').ReadOnlyPD


class Index {

  constructor(name, opts = {}) {
    var {keyer, unique = false, comparator = compareThings, keepNonIndexables=false} = opts
    if (!keyer && typeof opts === 'function') {
      // short hand in form of (name, keyer)
      keyer = opts
    }
    if (typeof keyer !== 'function') {
      throw new Error('keyer function required')
    }

    // disallow changing of these fields
    Object.defineProperties(this, {
      name: new ReadOnlyPD(name),
      unique: new ReadOnlyPD(unique),
      keepNonIndexables: new ReadOnlyPD(keepNonIndexables)
    })

    // harden changing of these fields by using symbol index for it
    this[$cmpr$] = comparator
    this[$keyer$] = keyer

    this[$reset$]()
  }

  [$reset$]() {
    this[$tree$] = new SearchTree({unique: this.unique, compareKeys: this[$cmpr$]})
    this[$keys$] = new WeakMap()
    this.nonIndexables = this.keepNonIndexables ? new Set() : null
  }

  [$add$](doc) {
    var key = this[$keyer$](doc)
    logger.trace({key, doc, idx: this}, 'adding to index')
    if (key === undefined || key === null) {
      logger.debug({key: typeof key, doc, index: this}, 'index keyer got undefined/null key for doc to add')
      if (this.nonIndexables) this.nonIndexables.add(doc)
    } else {
      this[$tree$].insert(key, doc)
      this[$keys$].set(doc, key)
    }
  }

  [$upd$](doc) {
    // delete old key info
    var key = this[$keys$].get(doc)
    logger.trace({key, doc, idx: this}, 'updating index')
    if (key === undefined) {
      logger.debug({key: typeof key, doc, index: this}, 'index has undefined/null key for doc to update')
      if (this.nonIndexables) this.nonIndexables.delete(doc)
    } else {
      this[$tree$].delete(key, doc)
      this[$keys$].delete(doc)
    }
    // add new key info
    this[$add$](doc)
  }

  [$del$](doc) {
    var key = this[$keys$].get(doc)
    logger.trace({key, doc, idx: this}, 'deleting from index')
    if (key === undefined || key === null) {
      logger.debug({key: typeof key, doc, index: this}, 'index has undefined/null key for doc to delete')
      if (this.nonIndexables) this.nonIndexables.delete(doc)
    } else {
      this[$tree$].delete(key, doc)
      this[$keys$].delete(doc)
    }
  }

  deleteAllByKey(key) {
    if (key === undefined || key === null) {
      logger.debug({index: this}, 'delete non key from index')
    } else {
      this[$tree$].delete(key)
      // expect entry in this[$keys$] to be gc automatically
    }
  }

  deleteAll(doc) {
    var key = this[$keyer$](doc)
    if (key === undefined || key === null) {
      logger.debug({
        key: typeof key,
        doc,
        index: this
      }, 'index keyer got undefined/null key from example doc for deleteAll')
    } else {
      this[$tree$].delete(key)
    }
  }

  findByKey(key) {
    if (key === undefined || key === null) {
      logger.debug({key: typeof key, index: this}, 'search by undefined/null key')
      return []
    }
    return this[$tree$].search(key)
  }

  find(example) {
    var key = this[$keyer$](example)
    if (key === undefined || key === null) {
      logger.debug({
        key: typeof key,
        example,
        index: this
      }, 'index keyer got undefined/null key from example doc for search')
      return []
    }
    return this[$tree$].search(key)
  }

  /**
   * Get all docs whose key between bounds specified as key(s)
   * Return it in key order
   * @param {Object} criteria Mongo-style query where keys are $lt, $lte, $gt or $gte (other keys are not considered)
   */
  queryByKey(criteria) {
    return this[$tree$].betweenBounds(criteria)
  }

  /**
   * Get all docs whose key between bounds specified as example doc(s)
   * Return it in key order
   * @param {Object} criteria Mongo-style query where keys are $lt, $lte, $gt or $gte (other keys are not considered)
   */
  query(criteria) {
    var keyBounds = {}
    for (var op in criteria) {
      var example = criteria[op]
      var key = this[$keyer$](example)
      if (key === undefined || key === null) {
        logger.debug({key: typeof key, example, index: this}, 'index keyer got undefined/null key from example')
        continue
      }
      keyBounds[op] = key
    }
    return this.queryByKey(keyBounds)
  }

}


function comparePrim(v1, v2) {
  if (v1 == v2) return 0
  if (v1 > v2) return 1
  if (v1 < v2) return -1
}

function compareThings(o1, o2) {
  if (o1 === o2) return 0
  if (!o1 || !o2) {
    // nulls last here
    if (o1) return -1
    if (o2) return 1
  }

  var result

  // try convert to primitive values and compare
  try {
    result = Number(comparePrim(o1.valueOf(), o2.valueOf()))
    if (isFinite(result))return result
  } catch (err) {
    logger.warn({err})
  }

  // try convert to strings and compare
  try {
    result = Number(comparePrim(o1.toString(), o2.toString()))
    if (isFinite(result))return result
  } catch (err) {
    logger.warn({err})
  }

  // still not comparable, resort to type name comparation,
  // so that same type are considered equal,
  // where as diff types are sorted by type name
  logger.debug({o1, o2}, 'Hard Comparation')
  result = Number(comparePrim(typeof o1, typeof o2))
  if (isFinite(result)) return result

  // even typeof is not comparable ?!
  logger.fatal({t1: typeof o1, t2: typeof o2}, 'Comparation Panic')
  throw new Error('Unable to compare [' + typeof o1 + '] and [' + typeof o2 + ']')
}


module.exports = Collection


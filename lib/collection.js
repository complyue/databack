'use strict';

const fs = require('fs')
const path = require('path')
const EventEmitter = require('events')
const crypto = require('crypto')

const logger = require('./log').ofModule(module)
const SYMS = require('./symbols')

const Index = require('./index')
const Document = require('./document')
const Persis = require('./persis')
const odm = require('./odm')


function defaultIdGen() {

  function d2(i) {
    if (i < 10) return '0' + i
    else return '' + i
  }

  var d = new Date()
  return d.getFullYear() + d2(d.getMonth()) + d2(d.getDate())
    + '~' + String(Date.now()).substr(-8) + '~'
    + crypto.randomBytes(4).toString('base64').replace(/[+\/]/g, '_').substr(0, 5);
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
    this[SYMS.persis] = new Persis(this.filename, this)
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
                if (!ds.emit('error', err)) {
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
    for (var [id, doc] of this.allDocs) {
      for (idx of idx2load) {
        idx[SYMS.add](doc)
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
        idx[SYMS.add](doc)
      }
      docs.push(doc)
    }
    this.save(docs, cb)
    return Array.isArray(arguments[0]) ? docs : docs[0]
  }

  /**
   * save updates on docs in this collection
   *
   * @param doc
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
        idx[SYMS.upd](doc)
      }
      // serialize doc to a data line
      payload.push(odm.serialize(doc))
    }
    this[SYMS.persis].queue(payload.join('\n') + '\n', (err)=> {
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
   *
   * compact persistent storage backing this collection
   *
   */
  compact(cb) {
    const handleErr = (err)=> {
      if (!err)
        return true

      logger.errror({err})

      // resume normal persis, and backlog should retain as not successfully persisted by the compact
      this[SYMS.persis].resume(false)

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
    this[SYMS.persis].standby((err)=> {
      if (!handleErr(err)) return

      // take the snapshot data of all docs at this point
      var payload = []
      for (var [id, doc] of this.allDocs) {
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
          // ignore err mostly be file not exists
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
                this[SYMS.persis].resume(true)
                // notify callback and listeners
                if (cb) {
                  cb.call(this, null)
                }
                this.emit('compact')
              })
            })
          })
        })
      })

    })

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
        idx[SYMS.del](doc)
      }
      ids.push(doc.$id$)
    }
    var payload = []
    for (var id of ids) {
      payload.push(odm.opDelete(id))
    }
    this[SYMS.persis].queue(payload.join('\n') + '\n', (err) => {
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


module.exports = Collection

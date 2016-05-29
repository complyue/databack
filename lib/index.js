'use strict';

const BTree = require('binary-search-tree').AVLTree

const logger = require('./log').ofModule(module)

const SYMS = require('./symbols')


class Index {

  constructor(name, opts = {}) {
    var {sampler, unique = false, comparator = compareThings} = opts
    if (!sampler && typeof opts === 'function') {
      // short hand when (name, sampler) is passed
      sampler = opts
    }
    if (typeof sampler !== 'function') {
      throw new Error('sampler function required')
    }
    this.name = name
    this.unique = unique
    this.comparator = comparator
    this[SYMS.sampler] = sampler

    this[SYMS.reset]()
  }

  [SYMS.reset]() {
    this[SYMS.tree] = new BTree({unique: this.unique, compareKeys: this.comparator})
    this[SYMS.keys] = new WeakMap()
    this.nonDocs = new Set()
  }

  [SYMS.add](doc) {
    var key = this[SYMS.sampler](doc)
    logger.trace({key, doc, idx: this}, 'adding to index')
    if (key === undefined || key === null) {
      logger.debug({key: typeof key, doc, index: this}, 'index sampler got undefined/null key for doc to add')
      this.nonDocs.add(doc)
    } else {
      this[SYMS.tree].insert(key, doc)
      this[SYMS.keys].set(doc, key)
    }
  }

  [SYMS.upd](doc) {
    // delete old key info
    var key = this[SYMS.keys].get(doc)
    logger.trace({key, doc, idx: this}, 'updating index')
    if (key === undefined) {
      logger.debug({key: typeof key, doc, index: this}, 'index has undefined/null key for doc to update')
      this.nonDocs.delete(doc)
    } else {
      this[SYMS.tree].delete(key, doc)
      this[SYMS.keys].delete(doc)
    }
    // add new key info
    this[SYMS.add](doc)
  }

  [SYMS.del](doc) {
    var key = this[SYMS.keys].get(doc)
    logger.trace({key, doc, idx: this}, 'deleting from index')
    if (key === undefined || key === null) {
      logger.debug({key: typeof key, doc, index: this}, 'index has undefined/null key for doc to delete')
      this.nonDocs.delete(doc)
    } else {
      this[SYMS.tree].delete(key, doc)
      this[SYMS.keys].delete(doc)
    }
  }

  deleteAllByKey(key) {
    if (key === undefined || key === null) {
      logger.debug({index: this}, 'delete non key from index')
    } else {
      this[SYMS.tree].delete(key)
      // expect entry in this[SYMS.keys] to be gc automatically
    }
  }

  deleteAll(doc) {
    var key = this[SYMS.sampler](doc)
    if (key === undefined || key === null) {
      logger.debug({
        key: typeof key,
        doc,
        index: this
      }, 'index sampler got undefined/null key from example doc for deleteAll')
    } else {
      this[SYMS.tree].delete(key)
    }
  }

  searchByKey(key) {
    if (key === undefined || key === null) {
      logger.debug({key: typeof key, index: this}, 'search by undefined/null key')
      return []
    }
    return this[SYMS.tree].search(key)
  }

  search(doc) {
    var key = this[SYMS.sampler](doc)
    if (key === undefined || key === null) {
      logger.debug({
        key: typeof key,
        doc,
        index: this
      }, 'index sampler got undefined/null key from example doc for search')
      return []
    }
    return this[SYMS.tree].search(key)
  }

  /**
   * Get all docs whose key between bounds specified as key(s)
   * Return it in key order
   * @param {Object} query Mongo-style query where keys are $lt, $lte, $gt or $gte (other keys are not considered)
   */
  betweenKeyBounds(criteria) {
    return this[SYMS.tree].betweenBounds(criteria)
  }

  /**
   * Get all docs whose key between bounds specified as example doc(s)
   * Return it in key order
   * @param {Object} query Mongo-style query where keys are $lt, $lte, $gt or $gte (other keys are not considered)
   */
  betweenBounds(criteria) {
    var keyBounds = {}
    for (var op in criteria) {
      var example = criteria[op]
      var key = this[SYMS.sampler](example)
      if (key === undefined || key === null) {
        logger.debug({key: typeof key, example, index: this}, 'index sampler got undefined/null key from example')
        continue
      }
      keyBounds[op] = key
    }
    return this.betweenKeyBounds(keyBounds)
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

  // try convert to primitive values and compare
  try {
    var result = Number(comparePrim(o1.valueOf(), o2.valueOf()))
    if (isFinite(result))return result
  } catch (err) {
    logger.warn({err})
  }

  // try convert to strings and compare
  try {
    var result = Number(comparePrim(o1.toString(), o2.toString()))
    if (isFinite(result))return result
  } catch (err) {
    logger.warn({err})
  }

  // still not comparable, resort to type name comparation,
  // so that same type are considered equal,
  // where as diff types are sorted by type name
  logger.debug({o1, o2}, 'Hard Comparation')
  var result = Number(comparePrim(typeof o1, typeof o2))
  if (isFinite(result)) return result

  // even typeof is not comparable ?!
  logger.fatal({t1: typeof o1, t2: typeof o2}, 'Comparation Panic')
  throw new Error('Unable to compare [' + typeof v1 + '] and [' + typeof v2 + ']')
}


module.exports = Index

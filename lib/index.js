'use strict';


const BST = require('binary-search-tree')
const SearchTree = BST.AVLTree

const packageLogger = require('./log')
const logger = packageLogger.ofModule(module)

const SYMS = require('./symbols')

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
    this[SYMS.cmpr] = comparator
    this[SYMS.keyer] = keyer

    this[SYMS.reset]()
  }

  [SYMS.reset]() {
    this[SYMS.tree] = new SearchTree({unique: this.unique, compareKeys: this[SYMS.cmpr]})
    this[SYMS.keys] = new WeakMap()
    this.nonIndexables = this.keepNonIndexables ? new Set() : null
  }

  [SYMS.add](doc) {
    var key = this[SYMS.keyer](doc)
    logger.trace({key, doc, idx: this}, 'adding to index')
    if (key === undefined || key === null) {
      logger.debug({key: typeof key, doc, index: this}, 'index keyer got undefined/null key for doc to add')
      if (this.nonIndexables) this.nonIndexables.add(doc)
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
      if (this.nonIndexables) this.nonIndexables.delete(doc)
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
      if (this.nonIndexables) this.nonIndexables.delete(doc)
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
    var key = this[SYMS.keyer](doc)
    if (key === undefined || key === null) {
      logger.debug({
        key: typeof key,
        doc,
        index: this
      }, 'index keyer got undefined/null key from example doc for deleteAll')
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
    var key = this[SYMS.keyer](doc)
    if (key === undefined || key === null) {
      logger.debug({
        key: typeof key,
        doc,
        index: this
      }, 'index keyer got undefined/null key from example doc for search')
      return []
    }
    return this[SYMS.tree].search(key)
  }

  /**
   * Get all docs whose key between bounds specified as key(s)
   * Return it in key order
   * @param {Object} criteria Mongo-style query where keys are $lt, $lte, $gt or $gte (other keys are not considered)
   */
  betweenKeyBounds(criteria) {
    return this[SYMS.tree].betweenBounds(criteria)
  }

  /**
   * Get all docs whose key between bounds specified as example doc(s)
   * Return it in key order
   * @param {Object} criteria Mongo-style query where keys are $lt, $lte, $gt or $gte (other keys are not considered)
   */
  betweenBounds(criteria) {
    var keyBounds = {}
    for (var op in criteria) {
      var example = criteria[op]
      var key = this[SYMS.keyer](example)
      if (key === undefined || key === null) {
        logger.debug({key: typeof key, example, index: this}, 'index keyer got undefined/null key from example')
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


module.exports = Index

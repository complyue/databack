'use strict';

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
module.exports.cmpr = Symbol('$cmpr$')
module.exports.keyer = Symbol('$keyer$')
module.exports.tree = Symbol('$tree$')
module.exports.keys = Symbol('$keys$')
module.exports.add = Symbol('$add$')
module.exports.upd = Symbol('$upd$')
module.exports.del = Symbol('$del$')
module.exports.reset = Symbol('$reset$')

// for Collection
module.exports.persis = Symbol('$persis$')

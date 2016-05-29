'use strict';

// shared symbols as hidden property keys not meant to be used by client code


// for Index
module.exports.sampler = Symbol('$sampler$')
module.exports.tree = Symbol('$tree$')
module.exports.keys = Symbol('$keys$')
module.exports.add = Symbol('$add$')
module.exports.del = Symbol('$del$')
module.exports.reset = Symbol('$reset$')

// for Collection
module.exports.persis = Symbol('$persis$')
module.exports.save = Symbol('$save$')

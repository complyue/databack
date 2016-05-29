'use strict';


const SYMS = require('./symbols')


function HiddenReadOnlyPD(value) {
  this.value = value
}
HiddenReadOnlyPD.prototype = {
  configurable: false,
  enumerable: false,
  writable: false
}


function Document(ds, id) {
  Object.defineProperties(this, {

    $id$: new HiddenReadOnlyPD(id),

    $save$: new HiddenReadOnlyPD(function (cb) {
      ds.save(this, cb)
    }),

    $delete$: new HiddenReadOnlyPD(function (cb) {
      ds.delete(this, cb)
    })
  })
}


module.exports = Document

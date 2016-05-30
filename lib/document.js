'use strict';

const ReadOnlyPD = require('./props').ReadOnlyPD
const HiddenReadOnlyPD = require('./props').HiddenReadOnlyPD


function Document(ds, id) {
  Object.defineProperties(this, {
    $id$: new ReadOnlyPD(id),
    $ds$: new HiddenReadOnlyPD(ds)
  })
}

Object.defineProperties(Document.prototype, {
  $save$: new ReadOnlyPD(function (cb) {
    this.$ds$.save(this, cb)
  }),
  $delete$: new ReadOnlyPD(function (cb) {
    this.$ds$.delete(this, cb)
  })
})


module.exports = Document


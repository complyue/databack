'use strict';

const props = require('./props')


function Document(ds, id) {
  Object.defineProperties(this, {
    $id$: new props.ReadOnlyPD(id),
    $ds$: new props.HiddenReadOnlyPD(ds)
  })
}

Object.defineProperties(Document.prototype, {
  $save$: new props.ReadOnlyPD(function (cb) {
    this.$ds$.save(this, cb)
  }),
  $delete$: new props.ReadOnlyPD(function (cb) {
    this.$ds$.delete(this, cb)
  })
})


module.exports = Document


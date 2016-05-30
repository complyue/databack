'use strict';


function ReadOnlyPD(value) {
  this.value = value
}
ReadOnlyPD.prototype = {
  configurable: false,
  enumerable: true,
  writable: false
}


function HiddenReadOnlyPD(value) {
  this.value = value
}
HiddenReadOnlyPD.prototype = {
  configurable: false,
  enumerable: false,
  writable: false
}


module.exports.ReadOnlyPD = ReadOnlyPD
module.exports.HiddenReadOnlyPD = HiddenReadOnlyPD

'use strict';

const fs = require('fs')


class Persis {

  constructor(filename, ds) {
    this.filename = filename
    this.ds = ds
    this._queue = []
    this._backlog = []
    this.currTask = null
    this._standby = false
    this._standbyCBs = null
    this.lastErr = null
  }

  _sched() {
    if (this.currTask) {
      // max 1 task in working
      return
    }

    // last task finished

    // check standby mode
    if (this._standby) {
      // in standby mode, start 1 standby cycle
      var sbcb = this._standbyCBs.shift()
      // notify standby callbacks
      sbcb(this.lastErr)
      return
    }

    // not in standby mode, check next task
    var task = this._queue.shift()
    if (!task) {
      // idle now
      if (this.ds) {
        this.ds.emit('idle')
      }
      return
    }

    // start this task
    this.currTask = task
    fs.appendFile(this.filename, task.payload, 'utf-8', (err)=> {
      this.lastErr = err
      if (err) {
        if (task.cb) {
          task.cb(err)
        }
      } else {
        // notify callback
        if (task.cb) {
          task.cb(null)
        }
      }

      this.currTask = null
      // schedule more tasks
      this._sched()
    })
  }

  queue(payload, cb) {
    this._queue.push({payload, cb})
    this._sched()
  }

  standby(cb) {
    this._standby = true
    if (!this._standbyCBs)
      this._standbyCBs = []
    this._standbyCBs.push(cb)
    this._backlog = this._queue
    this._queue = []
    this._sched()
  }

  resume(discardBacklog) {
    if (!discardBacklog) {
      // prepend backlog to queue head
      this._queue = this._backlog.concat(this.queue)
    }
    this._backlog = []

    if (this._standbyCBs && this._standbyCBs.length > 0) {
      // more standby cycles to run, keep standby status
      this._backlog = this._queue
      this._queue = []
    } else {
      // really leaving standby mode
      this._standby = false
      this._standbyCBs = null
    }

    this._sched()
  }

}


module.exports = Persis

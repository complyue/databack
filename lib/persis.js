'use strict';

const fs = require('fs')

const packageLogger = require('./log')
const logger = packageLogger.ofModule(module)


class Persis {

  constructor(filename, ds) {
    this.filename = filename
    this.ds = ds
    this._queue = []
    this._backlog = []
    this.currTask = null
    this._standby = false
    this._preceders = null
    this._currPreceder = null
    this.lastErr = null
  }

  _doTask(task, cb) {
    fs.open(this.filename, 'a', 0o640, (err, fd)=> {
      if (err) return cb(err)
      fs.write(fd, task.payload, null, 'utf-8', (errWrite)=> {
        if (task.sync) {
          fs.fsync(fd, (errSync)=> {
            fs.close(fd, (errClose)=> {
              cb(errWrite || errSync || errClose)
            })
          })
        } else {
          fs.close(fd, (errClose)=> {
            cb(errWrite || errClose)
          })
        }
      })
    })
  }

  _sched() {
    if (this.currTask) {
      // max 1 task in working
      return
    }

    // last task finished

    if (this._standby) {
      // in standby mode
      if (!this._currPreceder) {
        // in standby mode, start 1 standby cycle
        var preceder = this._preceders.shift()
        // run preceder, it should start if work and call resume() when done
        preceder(this.lastErr)
        this._currPreceder = preceder
      } else {
        // preceder not finished yet
      }
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
    this._doTask(task, (err)=> {
      this.lastErr = err
      if (err) {
        if (task.cb) {
          task.cb(err)
        } else {
          throw new Error('no callback passed for error: ' + err.stack)
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

  queue(payload, cb, sync = false) {
    this._queue.push({payload, cb, sync})
    this._sched()
  }

  standby(cbPreceder) {
    this._standby = true
    if (!this._preceders)
      this._preceders = []
    this._preceders.push(cbPreceder)
    this._backlog = this._queue
    this._queue = []
    this._sched()
  }

  resume(cbPreceder, discardBacklog) {
    if (!discardBacklog) {
      // prepend backlog to queue head
      this._queue = this._backlog.concat(this.queue)
    }
    this._backlog = []

    if (cbPreceder !== this._currPreceder) {
      var err = new Error('resuming with unpaired callback')
      logger.fatal({err})
      throw err
    }
    this._currPreceder = null

    if (this._preceders && this._preceders.length > 0) {
      // more preceders to run, keep standby status
      this._backlog = this._queue
      this._queue = []
    } else {
      // really leaving standby mode
      this._standby = false
      this._preceders = null
    }

    this._sched()
  }

}


module.exports = Persis

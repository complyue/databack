'use strict';

const logger = require('./lib/log')

const Collection = require('./lib/collection')


module.exports.Collection = Collection


if (module === require.main) {

  const searchByAge = function (ds, msg, criteria = {},
                                result = ds.indices.age.betweenKeyBounds(criteria),
                                ids = result.map(d=>d.$id$)) {
    logger.info({criteria, result}, msg)
    return result
  }

  const searchByName = function (ds, msg, name,
                                 result = ds.indices.name.searchByKey(name),
                                 ids = result.map(d=>d.$id$)) {
    logger.info({result, matchName: name}, msg)
    return result
  }

  const dsPoC = function () {
    var ds = this

    if (ds.allDocs.size < 4) {
      ds.add([{name: 'Compl', age: 37}, {name: 'Yue', age: 25}], (err, docs)=> {
        logger.info({err, docs}, 'added')
      })
    }
    if (ds.allDocs.size <= 2) {
      ds.add([{name: 'no-age'}, {age: 'secret'}], (err, docs)=> {
        logger.info({err, docs}, 'added')
      })
    }

    logger.info({population: ds.allDocs.size}, 'initial collection')
    console.log('all docs', ds.allDocs)

    var [d1,d2] =  searchByAge(ds, 'age 20 ~ 90', {$gt: 20, $lte: 90})

    if (d1) {
      d1.age = 28
      d1.$save$((err, docs)=> {
        logger.info({err, d1}, 'saved')
      })
      searchByAge(ds, 'after update, age more than 26', {$gt: 26})
    }

    if (d2) {
      d2.$delete$((err, ids)=> {
        logger.info({err, ids}, 'deleted')
      })
      searchByAge(ds, 'after delete, age less than 90', {$lte: 90})
    }

    searchByName(ds, 'result named', 'Yue')

    /*
     ds.compact((err)=> {
     logger.info({err, population: ds.allDocs.size}, 'compact done')
     })
     */

    ds.once('idle', ()=> {
      logger.info({population: ds.allDocs.size}, 'final collection')
      console.log('all docs', ds.allDocs)
    })
  }

  new Collection('ds1.txt', {
    onload: dsPoC,
    indices: {
      name: d=>d.name,
      age: d=>Number(d.age) || undefined
    }
  })

}

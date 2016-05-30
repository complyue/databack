'use strict';

const logger = require('./lib/log')

const Collection = require('./lib/collection')


module.exports.Collection = Collection


if (module === require.main) {

  const searchByAge = function (ds, msg, criteria = {},
                                result = ds.indices.age.betweenKeyBounds(criteria)) {
    logger.info({criteria, result}, msg)

    return result
  }


  const searchByBirth = function (ds, msg, criteria = {},
                                  result = ds.indices.birth.betweenKeyBounds(criteria)) {
    logger.info({criteria, result}, msg)
    return result
  }


  const searchByBirthExample = function (ds, msg, criteria = {},
                                         result = ds.indices.birth.betweenBounds(criteria)) {
    logger.info({criteria, result}, msg)
    return result
  }


  const searchByName = function (ds, msg, name,
                                 result = ds.indices.name.searchByKey(name)) {
    logger.info({result, matchName: name}, msg)
    return result
  }


  const dsPoC = function () {
    var ds = this

    if (ds.allDocs.size <= 3) {
      ds.add([{
        name: 'Compl', age: 37, birth: new Date(1979, 10, 15)
      }, {
        name: 'Ting', age: 25, birth: new Date(1990, 9, 27)
      }], (err, docs)=> {
        logger.info({err, docs}, 'added')
      })
    }
    if (ds.allDocs.size < 3) {
      // some non-indexables
      ds.add([{name: 'no-age'}, {age: 'secret'}], (err, docs)=> {
        logger.info({err, docs}, 'added')
      })
    }

    logger.info({population: ds.allDocs.size}, 'initial collection')
    console.log('all docs', ds.allDocs)

    var [d1] =  searchByAge(ds, 'age 20 ~ 90', {$gt: 20, $lte: 90})

    if (d1) {
      d1.age = 30
      d1.birth = new Date(1986, 2, 5)
      d1.$save$((err, docs)=> {
        logger.info({err, docs}, 'd1 saved')
      })
      searchByAge(ds, 'after update, age more than 28', {$gt: 28})
    }

    var Compls = ds.indices.name.searchByKey('Compl')
    var Tings = ds.indices.name.searchByKey('Ting')
    var todel = Compls.length > 1 ? Compls[0] : Tings.length > 1 ? Tings[0] : Compls[0]
    if (todel) {
      todel.$delete$((err, ids)=> {
        logger.info({err, ids}, 'deleted')
      })
      searchByAge(ds, 'after delete, age less than 90', {$lte: 90})
    }

    searchByName(ds, 'result named', 'Ting')

    /*
     ds.compact((err)=> {
     logger.info({err, population: ds.allDocs.size}, 'compact done')
     })
     */

    searchByBirth(ds, 'the 80s', {
      $gte: Date.UTC(1980, 0),
      $lt: Date.UTC(1990, 0)
    })

    searchByBirthExample(ds, 'the 70s', {
      $gte: {birth: new Date(1970, 0)},
      $lt: {birth: new Date(1980, 0)}
    })

    ds.once('idle', ()=> {
      logger.info({population: ds.allDocs.size}, 'final collection')
      console.log('all docs', ds.allDocs)
    })
  }


  new Collection('ds1.txt', {
    onload: dsPoC,
    indices: {
      name: d=>d.name,
      age: d=>Number(d.age) || undefined,
      birth: d=>d.birth instanceof Date ? d.birth.getTime() : undefined
    }
  })

}

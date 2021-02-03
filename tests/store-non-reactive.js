const test = require('tape')
const lmdb = require('node-lmdb')
const rimraf = require("rimraf")
const fs = require('fs')

const Store = require('../lib/Store.js')

const dbPath = `./test.nr.db`
rimraf.sync(dbPath)
fs.mkdirSync(dbPath)
const env = new lmdb.Env();
env.open({
  // Path to the environment
  path: dbPath,
  // Maximum number of databases
  maxDbs: 10
})
const dbi = env.openDbi({
  name: "test",
  create: true
})

test("store non-reactive properties", t => {
  t.plan(3)

  let store

  t.test("create store", async t => {
    t.plan(1)
    store = new Store(env, dbi)
    t.pass('store created')
  })

  t.test("non reactive operations", async t => {
    t.plan(19)

    t.test("put value", async t => {
      t.plan(1)
      await store.put({ id:'a', v: 1 })
      t.pass('value written')
    })

    t.test("get value", async t => {
      t.plan(1)
      const v = await store.objectGet('a')
      t.deepEqual(v, { v: 1, id: 'a' }, 'value read')
    })


    t.test("put another values", async t => {
      t.plan(1)
      await store.put({ id: 'c', v: 3 })
      await store.put({ id: 'b', v: 2 })
      t.pass('values written')
    })

    t.test("get range [a,c]", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gte: 'a', lte: 'c' })
      t.deepEqual(values, [ { v: 1, id: 'a' }, { v: 2, id: 'b' }, { v: 3, id: 'c' } ], 'range read' )
    })

    t.test("count range [a,c]", async t => {
      t.plan(1)
      let values = await store.countGet({ gte: 'a', lte: 'c' })
      t.deepEqual(values, 3, 'range count' )
    })

    t.test("get reverse range [c,a]", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gte: 'a', lte: 'c', reverse: true })
      t.deepEqual(values, [ { v: 3, id: 'c' }, { v: 2, id: 'b' },  { v: 1, id: 'a' } ], 'range read' )
    })

    t.test("count reverse range [a,c]", async t => {
      t.plan(1)
      let values = await store.countGet({ gte: 'a', lte: 'c', reverse: true })
      t.deepEqual(values, 3, 'range count' )
    })

    t.test("get range [a,c] with limit 2", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gte: 'a', lte: 'c', limit: 2 })
      t.deepEqual(values, [ { v: 1, id: 'a' }, { v: 2, id: 'b' } ], 'range read' )
    })

    t.test("count range [a,c] with limit 2", async t => {
      t.plan(1)
      let values = await store.countGet({ gte: 'a', lte: 'c', limit: 2 })
      t.deepEqual(values, 2, 'range read' )
    })

    t.test("get reverse range [c,a] with limit 2", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gte: 'a', lte: 'c', reverse: true, limit: 2 })
      t.deepEqual(values, [ { v: 3, id: 'c' }, { v: 2, id: 'b' } ], 'range read' )
    })

    t.test("get reverse count [c,a] with limit 2", async t => {
      t.plan(1)
      let values = await store.countGet({ gte: 'a', lte: 'c', reverse: true, limit: 2 })
      t.deepEqual(values, 2, 'range read' )
    })

    t.test("get range (a,c]", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gt: 'a', lte: 'c' })
      t.deepEqual(values, [ { v: 2, id: 'b' }, { v: 3, id: 'c' } ], 'range read' )
    })

    t.test("get reverse range [c,a)", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gt: 'a', lte: 'c', reverse: true })
      t.deepEqual(values, [ { v: 3, id: 'c' }, { v: 2, id: 'b' }, ], 'range read' )
    })

    t.test("get range [a,c)", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gte: 'a', lt: 'c' })
      t.deepEqual(values, [ { v: 1, id: 'a' }, { v: 2, id: 'b' } ], 'range read' )
    })

    t.test("get reverse range (c,a]", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gte: 'a', lt: 'c', reverse: true })
      t.deepEqual(values, [ { v: 2, id: 'b' }, { v: 1, id: 'a' } ], 'range read' )
    })

    t.test("get range (a,c)", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gt: 'a', lt: 'c' })
      t.deepEqual(values, [ { v: 2, id: 'b' } ], 'range read' )
    })

    t.test("get reverse range (a,c)", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gt: 'a', lt: 'c', reverse: true })
      t.deepEqual(values, [ { v: 2, id: 'b' } ], 'range read' )
    })

    t.test("remove 'b'", async t => {
      t.plan(1)
      await store.delete('b')
      t.pass("removed")
    })

    t.test("get range [a,c]", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gte: 'a', lte: 'c' })
      t.deepEqual(values, [ { v: 1, id: 'a' }, { v: 3, id: 'c' } ], 'range read' )
    })
  })

  t.test("close and remove database", async t => {
    t.plan(1)
    dbi.close()
    env.close()
    rimraf(dbPath, (err) => {
      if(err) return t.fail(err)
      t.pass('removed')
    })
  })
})

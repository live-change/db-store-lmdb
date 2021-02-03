const test = require('tape')
const lmdb = require('node-lmdb')
const rimraf = require("rimraf")
const fs = require("fs")

const Store = require('../lib/Store.js')

const dbPath = `./test.ro.db`
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

test("store count observable", t => {
  t.plan(5)

  let store

  t.test("create store", async t => {
    t.plan(1)
    store = new Store(env, dbi)
    t.pass('store created')
  })

  t.test("put objects", async t => {
    t.plan(1)
    await store.put({ v: 1, id: 'a' })
    await store.put({ v: 3, id: 'c' })
    t.pass('objects written')
  })

  let nextValueResolve
  let gotNextValue
  const getNextValue = () => {
    if(gotNextValue) {
      gotNextValue = false
      return countObservable.value
    }
    return new Promise((resolve, reject) => nextValueResolve = resolve)
  }
  let countObservable
  const countObserver = (signal, value, ...rest) => {
    console.log("SIGNAL", signal, value, ...rest)
    gotNextValue = true
    if(nextValueResolve) nextValueResolve(countObservable.value)
  }

  t.test("observe count [a,z]", async t => {
    t.plan(6)
    countObservable = store.countObservable({ gte: 'a', lte: 'z' })
    countObservable.observe(countObserver)
    let values = await getNextValue()
    t.deepEqual(values, 2, 'count value' )

    t.test("remove object 'a' from observed count", async t => {
      t.plan(1)
      await store.delete('a')
      let values = await getNextValue()
      t.deepEqual(values, 1, 'count value' )
    })

    t.test("add object 'a' to observed count", async t => {
      t.plan(1)
      await store.put({ id: 'a', v: 4 })
      let values = await getNextValue()
      t.deepEqual(values, 2, 'count value' )
    })

    t.test("add object 'b' to observed count", async t => {
      t.plan(1)
      await store.put({ id: 'b', v: 5 })
      let values = await getNextValue()
      t.deepEqual(values, 3, 'count value' )
    })

    t.test("add object 'd' to observed count", async t => {
      t.plan(1)
      await store.put({ id: 'd', v: 6 })
      let values = await getNextValue()
      t.deepEqual(values, 4, 'count value' )
    })

    t.test("unobserve count", async t => {
      t.plan(1)
      countObservable.unobserve(countObserver)
      t.pass('unobserved')
    })
  })

  t.test("observe count (a,d)", async t => {
    t.plan(6)
    countObservable = store.countObservable({ gt: 'a', lt: 'd' })
    countObservable.observe(countObserver)
    let values = await getNextValue()
    t.deepEqual(values, 2, 'count value' )

    t.test("remove object 'd' outside observed count", async t => {
      t.plan(1)
      await store.delete('d')
      t.pass('deleted')
    })

    t.test("remove object 'a' outside observed count", async t => {
      t.plan(1)
      await store.delete('a')
      t.pass('deleted')
    })

    t.test("add object 'ab' to observed count", async t => {
      t.plan(1)
      await store.put({ id: 'ab', v: 7 })
      let values = await getNextValue()
      t.deepEqual(values, 3, 'count value' )
    })

    t.test("remove object 'ab' from observed count", async t => {
      t.plan(1)
      await store.delete('ab')
      let values = await getNextValue()
      t.deepEqual(values, 2, 'count value' )
    })

    t.test("unobserve count", async t => {
      t.plan(1)
      countObservable.unobserve(countObserver)
      t.pass('unobserved')
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

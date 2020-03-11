const test = require('tape')
const lmdb = require('node-lmdb')
const rimraf = require("rimraf")
const fs = require("fs")

const Store = require('../lib/Store.js')

const dbPath = `./test.oo.db`
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

test("store object observable", t => {
  t.plan(3)

  let store

  t.test("create store", async t => {
    t.plan(1)
    store = new Store(env, dbi)
    t.pass('store created')
  })

  let nextValueResolve
  let gotNextValue
  const getNextValue = () => {
    if(gotNextValue) {
      gotNextValue = false
      return objectObservable.value
    }
    return new Promise((resolve, reject) => nextValueResolve = resolve)
  }

  let objectObservable
  const objectObserver = (signal, value, ...rest) => {
    console.log("SIGNAL", signal, value, ...rest)
    gotNextValue = true
    if(nextValueResolve) nextValueResolve(value)
  }

  t.test('observe object A', async t => {
    t.plan(3)
    objectObservable = store.objectObservable('A')
    objectObservable.observe(objectObserver)
    let value = await getNextValue()
    t.deepEqual(value, null, 'found null')

    t.test("add object A", async t => {
      t.plan(1)
      await store.put({ id: 'A', a: 1 })
      let value = await getNextValue()
      t.deepEqual(value, { id: 'A', a: 1 } , 'found object' )
    })

    t.test("delete object A", async t => {
      t.plan(1)
      await store.delete('A')
      let value = await getNextValue()
      t.deepEqual(value, null , 'found null' )
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
const IntervalTree = require('node-interval-tree').default
const ReactiveDao = require("@live-change/dao")
const lmdb = require('node-lmdb')

class ObjectObservable extends ReactiveDao.ObservableValue {
  constructor(store, key) {
    super()
    this.store = store
    this.key = key

    this.disposed = false
    this.ready = false
    this.respawnId = 0

    this.forward = null

    this.readPromise = this.startReading()
  }

  async startReading() {
    this.store.objectObservables.set(this.key, this)
    this.value = await this.store.objectGet(this.key)
    this.fireObservers('set', this.value)
  }

  async set(value) {
    await this.readPromise
    this.value = value
    this.fireObservers('set', this.value)
  }

  dispose() {
    if(this.forward) {
      this.forward.unobserve(this)
      this.forward = null
      return
    }

    this.disposed = true
    this.respawnId++
    if(this.changesStream) this.changesStream.close()
    this.changesStream = null

    this.store.objectObservables.delete(this.key)
  }

  respawn() {
    const existingObservable = this.store.objectObservables.get(this.key)
    if(existingObservable) {
      this.forward = existingObservable
      this.forward.observe(this)
      return
    }

    this.respawnId++
    if(this.changesStream) this.changesStream.close()
    this.ready = false
    this.disposed = false
    this.startReading()
  }
}

class RangeObservable extends ReactiveDao.ObservableList {
  constructor(store, range) {
    super()
    this.store = store
    this.range = range

    this.disposed = false
    this.ready = false
    this.respawnId = 0
    this.refillId = 0
    this.refillPromise = null

    this.forward = null

    this.rangeKey = JSON.stringify(this.range)
    this.rangeDescr = [ this.range.gt || this.range.gte || '', this.range.lt || this.range.lte || '\xFF\xFF\xFF\xFF' ]

    this.readPromise = this.startReading()
  }

  async startReading() {
    this.store.rangeObservables.set(this.rangeKey, this)
    this.store.rangeObservablesTree.insert(...this.rangeDescr, this)
    this.set(await this.store.rangeGet(this.range))
  }

  async putObject(object, oldObject) {
    await this.readPromise
    const id = object.id
    if(this.range.gt && !(id > this.range.gt)) return
    if(this.range.lt && !(id < this.range.lt)) return
    if(!this.range.reverse) {
      if(this.range.limit && this.list.length == this.range.limit) {
        for(let i = 0, l = this.list.length; i < l; i++) {
          if(this.list[i].id == id) {
            this.list.splice(i, 1, object)
            this.fireObservers('putByField', 'id', id, object, false, oldObject)
            return
          } else if(this.list[i].id > id) {
            this.list.splice(i, 0, object)
            this.fireObservers('putByField', 'id', id, object, false, oldObject)
            const popped = this.list.pop()
            this.fireObservers('removeByField', 'id', popped.id, popped)
            return
          }
        }
      } else {
        this.putByField('id', object.id, object, false, oldObject)
      }
    } else {
      if(this.range.limit && this.list.length == this.range.limit) {
        for(let i = this.list.length-1; i >= 0; i--) {
          if(this.list[i].id == id) {
            this.list.splice(i, 1, object)
            this.fireObservers('putByField', 'id', id, object, true, oldObject)
            return
          } else if(this.list[i].id > id) {
            this.list.splice(i + 1, 0, object)
            this.fireObservers('putByField', 'id', id, object, true, oldObject)
            const popped = this.list.pop()
            this.fireObservers('removeByField', 'id', popped.id, popped)
            return
          }
        }
        this.list.splice(0, 0, object)
        this.fireObservers('putByField', 'id', id, object, true)
        const popped = this.list.pop()
        this.fireObservers('removeByField', 'id', popped.id, popped)
      } else {
        this.putByField('id', id, object, true, oldObject)
      }
    }
  }

  refillDeleted(from, limit) {
    this.refillId ++
    const refillId = this.refillId
    let promise = (async () => {
      let req
      if(!this.range.reverse) {
        req = { gt: from, limit }
        if(this.range.lt) req.lt = this.range.lt
        if(this.range.lte) req.lte = this.range.lte
      } else {
        req = { lt: from, limit, reverse: true }
        if(this.range.gt) req.gt = this.range.gt
        if(this.range.gte) req.gte = this.range.gte
      }
      const objects = await this.store.rangeGet(req)
      if(this.refillId != refillId) return this.refillPromise
      for(let object of objects) this.push(object)
      this.refillPromise = null
    })()
    this.refillPromise = promise
    return promise
  }

  async deleteObject(object) {
    if(!object) return;
    await this.readPromise
    const id = object.id
    if(this.range.gt && !(id > this.range.gt)) return
    if(this.range.lt && !(id < this.range.lt)) return
    if(this.range.limit && (this.list.length == this.range.limit || this.refillPromise)) {
      let exists
      let last
      for(let obj of this.list) {
        if(obj.id == id) exists = obj
        else last = obj
      }
      this.removeByField('id', id, object)
      if(exists) await this.refillDeleted(last.id, this.range.limit - this.list.length)
    } else {
      this.removeByField('id', id, object)
    }
  }

  dispose() {
    if(this.forward) {
      this.forward.unobserve(this)
      this.forward = null
      return
    }

    this.disposed = true
    this.respawnId++
    this.changesStream = null

    this.store.rangeObservables.delete(this.rangeKey)
    let removed = this.store.rangeObservablesTree.remove(...this.rangeDescr, this)
  }

  respawn() {
    const existingObservable = this.store.rangeObservables.get(JSON.stringify(this.range))
    if(existingObservable) {
      this.forward = existingObservable
      this.forward.observe(this)
      return
    }

    this.respawnId++
    this.ready = false
    this.disposed = false
    this.startReading()
  }
}

class Store {
  constructor(env, db) {
    this.env = env
    this.lmdb = db
    this.objectObservables = new Map()
    this.rangeObservables = new Map()
    this.rangeObservablesTree = new IntervalTree()
  }

  objectGet(key) {
    if(!key) throw new Error("key is required")
    const txn = this.env.beginTxn()
    let json
    try {
      json = txn.getString(this.lmdb, key)
    } catch(error) {

    } finally {
      txn.commit()
    }
    if(!json) return Promise.resolve(null)
    try {
      const obj = JSON.parse(json)
      return Promise.resolve(obj)
    } catch(e) {
      return Promise.reject(e)
    }
  }

  objectObservable(key) {
    let observable = this.objectObservables.get(key)
    if(observable) return observable
    observable = new ObjectObservable(this, key)
    return observable
  }

  rangeGet(range) {
    if(!range) throw new Error("range not defined")
    return new Promise((resolve, reject) => {
      let keys = []
      let data
      let found
      //console.log("TXN [")
      const txn = this.env.beginTxn()
      try {
        //console.log("  FETCH KEYS [")
        let cursor = new lmdb.Cursor(txn, this.lmdb)
        try {
          const min = range.gt || range.gte
          const max = range.lt || range.lte
          if(range.reverse) {
            if(max) {
              found = cursor.goToRange(max)
              if(!found) found = cursor.goToLast()
            } else {
              found = cursor.goToLast()
            }
            while((!range.limit || keys.length < range.limit) && found !== null) {
              if(range.gt && found <= range.gt) break;
              if(range.gte && found < range.gte) break;
              if((!range.lt || found < range.lt) && (!range.lte || found <= range.lte)) {
                // key in range, skip keys outside range
                keys.push(found)
              }
              found = cursor.goToPrev()
            }
          } else {
            if(min) {
              found = cursor.goToRange(min)
            } else {
              found = cursor.goToFirst()
            }
            while((!range.limit || keys.length < range.limit) && found !== null) {
              if(range.lt && found >= range.lt) break;
              if(range.lte && found > range.lte) break;
              if((!range.gt || found > range.gt) && (!range.gte || found >= range.gte)) {
                // key in range, skip keys outside range
                keys.push(found)
              }
              //console.log("    GO TO NEXT [")
              found = cursor.goToNext()
              //console.log("    ] GO TO NEXT")
            }
          }
        } finally {
          cursor.close()
        }
       // console.log("  ] FETCH KEYS")

        //console.log("  FETCH DATA [")
        data = new Array(keys.length)
        for(let i = 0; i < keys.length; i++) {
          const key = keys[i]
          const json = txn.getString(this.lmdb, key)
          try {
            const obj = JSON.parse(json)
            //obj.id = found
            data[i] = obj
          } catch(e) {
            return reject(e)
          }
        }
        //console.log("  ] FETCH DATA")
      } finally {
        txn.commit()
        //console.log("] TXN")
      }
      resolve(data)
    })
  }

  rangeObservable(range) {
    let observable = this.rangeObservables.get(JSON.stringify(range))
    if(observable) return observable
    observable = new RangeObservable(this, range)
    return observable
  }

  async put(object) {
    const id = object.id
    let oldObject = null
    const txn = this.env.beginTxn()
    try {
      const json = txn.getString(this.lmdb, id)
      oldObject = json ? JSON.parse(json) : null
      if (!id) {
        throw new Error("id must not be empty")
      }
      txn.putString(this.lmdb, id, JSON.stringify(object), {noOverwrite: false})
    } finally {
      txn.commit()
    }
    const objectObservable = this.objectObservables.get(id)
    if (objectObservable) objectObservable.set(object, oldObject)
    const rangeObservables = this.rangeObservablesTree.search(id, id)
    for (const rangeObservable of rangeObservables) {
      rangeObservable.putObject(object, oldObject)
    }
    return oldObject
  }

  async delete(id) {
    const txn = this.env.beginTxn()
    let object = null
    try {
      const json = txn.getString(this.lmdb, id)
      object = json ? JSON.parse(json) : null
      txn.del(this.lmdb, id)
    } catch(e) {
      console.error("FAILED REMOVE OF", id)
    } finally {
      txn.commit()
    }
    const objectObservable = this.objectObservables.get(id)
    if(objectObservable) objectObservable.set(null)
    const rangeObservables = this.rangeObservablesTree.search(id, id)
    for(const rangeObservable of rangeObservables) {
      rangeObservable.deleteObject(object)
    }
    return object
  }

}

module.exports = Store

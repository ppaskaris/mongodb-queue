/**
 *
 * mongodb-queue.js - Use your existing MongoDB as a local queue.
 *
 * Copyright (c) 2014 Andrew Chilton
 * - http://chilts.org/
 * - andychilton@gmail.com
 *
 * License: http://chilts.mit-license.org/2014/
 *
 **/

var crypto = require('crypto')

// some helper functions
function id() {
    return crypto.randomBytes(16).toString('hex')
}

function now() {
    return (new Date())
}

function nowPlusSecs(secs) {
    return (new Date(Date.now() + secs * 1000))
}

module.exports = function(mongoDbClient, name, opts) {
    return new Queue(mongoDbClient, name, opts)
}

// the Queue object itself
function Queue(mongoDbClient, name, opts) {
    if ( !mongoDbClient ) {
        throw new Error("mongodb-queue: provide a mongodb.MongoClient")
    }
    if ( !name ) {
        throw new Error("mongodb-queue: provide a queue name")
    }
    opts = opts || {}

    this.name = name
    this.col = mongoDbClient.collection(name)
    this.visibility = opts.visibility || 30
    this.delay = opts.delay || 0
    this.cleanAfterSeconds = opts.cleanAfterSeconds >= 0 ? opts.cleanAfterSeconds : -1

    if ( opts.deadQueue ) {
        this.deadQueue = opts.deadQueue
        this.maxRetries = opts.maxRetries || 5
    } else {
        this.maxRetries = opts.maxRetries || Infinity
    }
}

Queue.prototype.createIndexes = function(callback) {
    var self = this

    self.col.createIndex({ deleted : 1, visible : 1 }, function(err, indexname) {
        if (err) return callback(err)
        var opts1 = { unique : true, sparse : true }
        self.col.createIndex({ ack : 1 }, opts1, function(err) {
            if (err) return callback(err)
            if (self.cleanAfterSeconds < 0) {
                return callback(null, indexname)
            }
            var opts2 = { cleanAfterSeconds: self.cleanAfterSeconds }
            self.col.createIndex({ deleted: 1 }, opts2, function(err) {
                if (err) return callback(err)
                callback(null, indexname)
            })
        })
    })
}

Queue.prototype.migrate = function(callback) {
    var self = this

    var operations = []
    var addOperation = function(document) {
        var changes = {}
        if (typeof document.deleted === 'string') {
            changes.deleted = new Date(document.deleted)
        }
        if (typeof document.visible === 'string') {
            changes.visible = new Date(document.visible)
        }
        operations.push({
            updateOne: {
                filter: { _id: document._id },
                update: { $set: changes }
            }
        })
    }
    var bulkWrite = function(err) {
        if (err) return callback(err)
        if (operations.length <= 0) {
            return callback()
        }
        self.col.bulkWrite(operations, function(err, result) {
            if (err) return callback(err)
            callback(null, result.modifiedCount)
        })
    }

    var query = {
        $or: [
            { deleted: { $type: 2 } },
            { visible: { $type: 2 } }
        ]
    }
    self.col.find(query)
        .project({ deleted: 1, visible: 1 })
        .forEach(addOperation, bulkWrite)
}

Queue.prototype.add = function(payload, opts, callback) {
    var self = this
    if ( !callback ) {
        callback = opts
        opts = {}
    }
    var delay = opts.delay || self.delay
    var visible = delay ? nowPlusSecs(delay) : now()
    var debounce = opts.debounce || null

    var justOne = !Array.isArray(payload)
    var payloads = justOne ? [payload] : payload
    if (payloads.length <= 0) {
        var errMsg = 'Queue.add(): Array payload length must be greater than 0'
        return callback(new Error(errMsg))
    }
    var operations = payloads.map(function(payload) {
        if (debounce != null) {
            return {
                updateOne: {
                    filter: {
                        ack: null,
                        deleted: null,
                        debounce: debounce
                    },
                    update: {
                        $set: {
                            visible: visible,
                            payload: payload,
                        }
                    },
                    upsert: true
                }
            }
        } else {
            return {
                insertOne: {
                    visible: visible,
                    payload: payload,
                }
            }
        }
    })

    self.col.bulkWrite(operations, function(err, result) {
        if (err) return callback(err)
        var ids = payloads.map(function (_, index) {
            var id = result.insertedIds[index]
                || result.upsertedIds[index]
                || '(debounced)'
            return id
        })
        if (justOne) {
            return callback(null, '' + ids[0])
        } else {
            return callback(null, '' + ids)
        }
    })
}

Queue.prototype.get = function(opts, callback) {
    var self = this
    if ( !callback ) {
        callback = opts
        opts = {}
    }

    var visibility = opts.visibility || self.visibility
    var query = {
        deleted : null,
        visible : { $lte : now() },
    }
    var sort = {
        _id : 1
    }
    var update = {
        $inc : { tries : 1 },
        $set : {
            ack     : id(),
            visible : nowPlusSecs(visibility),
        }
    }

    self.col.findOneAndUpdate(query, update, { sort: sort, returnOriginal : false }, function(err, result) {
        if (err) return callback(err)
        var msg = result.value
        if (!msg) return callback()

        // convert to an external representation
        msg = {
            // convert '_id' to an 'id' string
            id      : '' + msg._id,
            ack     : msg.ack,
            payload : msg.payload,
            tries   : msg.tries,
        }
        // check the tries
        if ( msg.tries > self.maxRetries ) {
            // if we have a deadQueue, retire it into there
            if ( self.deadQueue ) {                // So:
                // 1) add this message to the deadQueue
                // 2) ack this message from the regular queue
                // 3) call ourself to return a new message (if exists)
                self.deadQueue.add(msg, function(err) {
                    if (err) return callback(err)
                    self.ack(msg.ack, function(err) {
                        if (err) return callback(err)
                        self.get(callback)
                    })
                })
                return
            }
            // otherwise, mark it as completed
            else {
                self.ack(msg.ack, function(err) {
                    if (err) return callback(err)
                    self.get(callback)
                })
            }
        }

        callback(null, msg)
    })
}

Queue.prototype.ping = function(ack, opts, callback) {
    var self = this
    if ( !callback ) {
        callback = opts
        opts = {}
    }

    var visibility = opts.visibility || self.visibility
    var query = {
        ack     : ack,
        visible : { $gt : now() },
        deleted : null,
    }
    var update = {
        $set : {
            visible : nowPlusSecs(visibility)
        }
    }
    self.col.findOneAndUpdate(query, update, { returnOriginal : false }, function(err, msg, blah) {
        if (err) return callback(err)
        if ( !msg.value ) {
            return callback(new Error("Queue.ping(): Unidentified ack  : " + ack))
        }
        callback(null, '' + msg.value._id)
    })
}

Queue.prototype.ack = function(ack, callback) {
    var self = this

    var query = {
        ack     : ack,
        visible : { $gt : now() },
        deleted : null,
    }
    var update = {
        $set : {
            deleted : now(),
        }
    }
    self.col.findOneAndUpdate(query, update, { returnOriginal : false }, function(err, msg, blah) {
        if (err) return callback(err)
        if ( !msg.value ) {
            return callback(new Error("Queue.ack(): Unidentified ack : " + ack))
        }
        callback(null, '' + msg.value._id)
    })
}

Queue.prototype.clean = function(callback) {
    var self = this

    var query = {
        deleted : { $exists : true },
    }

    self.col.deleteMany(query, callback)
}

Queue.prototype.total = function(callback) {
    var self = this

    self.col.count(function(err, count) {
        if (err) return callback(err)
        callback(null, count)
    })
}

Queue.prototype.size = function(callback) {
    var self = this

    var query = {
        deleted : null,
        visible : { $lte : now() },
    }

    self.col.count(query, function(err, count) {
        if (err) return callback(err)
        callback(null, count)
    })
}

Queue.prototype.inFlight = function(callback) {
    var self = this

    var query = {
        ack     : { $exists : true },
        visible : { $gt : now() },
        deleted : null,
    }

    self.col.count(query, function(err, count) {
        if (err) return callback(err)
        callback(null, count)
    })
}

Queue.prototype.done = function(callback) {
    var self = this

    var query = {
        deleted : { $exists : true },
    }

    self.col.count(query, function(err, count) {
        if (err) return callback(err)
        callback(null, count)
    })
}

var assert = require('assert')
const rochefort = require('./index.js')
var o


data = new Buffer.alloc(1024 * 1024 * 10)
r = new rochefort('http://localhost:8002')
r.append({data: data}).then(offset => {
    o = offset
    return r.get({offset: offset})
}).then(v => {
    assert.equal(0, Buffer.compare(data,v))
}).then(() => {
    return r.getMulti({offsets: [o,o,o,o]})
}).then(v => {
    assert.equal(4, v.length)
    for (var i = 0; i < v.length; i++) {
        assert.equal(0, Buffer.compare(v[i],data))
    }
}).catch(e => {
    console.log(e)
})

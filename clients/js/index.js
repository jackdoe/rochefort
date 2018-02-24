'use strict;'
const { URL } = require('url')
const axios = require('axios')

const append = function append(args) {
    return axios({
        method: 'get',
        url: new URL('append',args.url).toString(),
        params: {
            namespace: args.namespace,
            allocSize: args.allocSize,
        },
        timeout: args.timeout || 1000,
        data: args.data
    }).then((r) => {
        return r.data.offset
    })
}


const modify = function append(args) {
    return axios({
        method: 'get',
        url: new URL('modify',args.url).toString(),
        params: {
            namespace: args.namespace,
            pos: args.position,
            offset: args.offset,
        },
        timeout: args.timeout || 1000,
        data: args.data
    }).then((r) => {
        return r.data.success
    })
}


const get = function get(args) {
    return axios({
        method: 'get',
        url: new URL("get",args.url).toString(),
        params: {
            namespace: args.namespace,
            offset: args.offset || 0,
        },
        responseType: 'arraybuffer',
        timeout: args.timeout || 1000,
    }).then((r) => {
        return r.data
    })
}
const Buffer = require('buffer').Buffer;

const getMulti = function get(args) {
    var raw = Buffer.alloc(args.offsets.length * 8)
    var offsets = args.offsets
    for (let i = 0; i < offsets.length; i++) {
        var num = offsets[i]
        var bufferOffset = i * 8;

        var lo = num | 0;
        if (lo < 0)
            lo += 4294967296;

        var hi = num - lo;
        hi /= 4294967296;
        raw.writeUInt32LE(lo, bufferOffset)
        raw.writeUInt32LE(hi, bufferOffset + 4)
    }

    return axios({
        url:new URL("getMulti",args.url).toString(),
        params: {
            namespace: args.namespace,
        },
        responseType: 'arraybuffer',
        data: raw,
        timeout: args.timeout || 1000,
    }).then((r) => {
        var out = []
        var offset = 0
        var data = r.data
        while(offset < data.length) {
            var len = data.readUInt32LE(offset)
            out.push(data.slice(offset + 4, offset + len + 4))
            offset += 4 + len
        }
        return out
    })
}

const rochefort = function(url) {
    this.url = url
    this.get = function(args) {
        return get({...args, url: this.url})
    }

    this.append = function(args) {
        return append({...args, url: this.url})
    }

    this.modify = function(args) {
        return modify({...args, url: this.url})
    }

    this.getMulti = function(args) {
        return getMulti({...args, url: this.url})
    }
}

module.exports = rochefort

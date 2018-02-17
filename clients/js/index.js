'use strict;'
const { URL } = require('url')
const axios = require('axios')

const append = function append(args) {
    return axios({
        method: 'get',
        url: new URL('append',args.url).toString(),
        params: {
            namespace: args.namespace,
        },
        timeout: args.timeout || 1000,
        data: args.data
    }).then((r) => {
        return r.data.offset_str
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

const getMulti = function get(args) {
    return axios({
        url:new URL("getMulti",args.url).toString(),
        params: {
            csv: "true",
            namespace: args.namespace,
        },
        responseType: 'arraybuffer',
        data: (args.offsets || []).join(","),
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

    this.getMulti = function(args) {
        return getMulti({...args, url: this.url})
    }
}

module.exports = rochefort

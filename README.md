# rochefort - poor man's kafka (with in-place modifications and inverted index search)

### PUSH DATA, GET FILE OFFSET; no shenanigans
#### (if you can afford to lose data and do your own replication)

* **disk write speed** storage service that returns offsets to stored values
* if you are ok with losing some data (does not fsync on write)
* supports: **append, multiappend, modify, get, multiget, close, query, compact**
* clients: [go](https://github.com/jackdoe/go-rochefort-client), [java](https://github.com/jackdoe/rochefort/tree/master/clients/java)

## turns out when you are fine with losing some data, things are much faster and simpler :)

---

## run in docker

run with docker: jackdoe/rochefort:2.1

```
docker run -e BIND=":8000" \
           -e ROOT="/tmp/rochefort" \
           -p 8000:8000 \
           jackdoe/rochefort:2.1
```

### breaking change between 0.5 and 1.0

* added 4 more bytes in the header
* the -buckets parameter is gone, so everything is appended in one file per namespace

you can migrate your data by doing:

```
oldServer.scan(namespace: ns) do |offset, v|
  newServer.append(namespace:ns, data: v)
end
```

### breaking change between 1.x and 2.0

* moved get/multiget/append to protobuf
* moved delete/close to protobuf


### parameters
* root: root directory, files will be created at `root/namespace||default/append.raw`
* bind: address to bind to (default :8000)

dont forget to mount persisted root directory


## compile from source

```
$ go run main.go query.go input.pb.go -bind :8000 -root /tmp
2018/02/10 12:06:21 starting http server on :8000
....

```

## APPEND/MULTI APPEND

```
res, err := r.Set(&AppendInput{
	AppendPayload: []*Append{{
		Namespace: ns,
		Data:      []byte("abc"),
                AllocSize: 10, // so you can do inplace modification
                Tags:      []string{"a","b","c"} // so you can search it
	}, {
		Namespace: ns,
		Data:      []byte("zxc"),
	}},
})
```


you can always do inplace modifications to an object, and you can also reserve some space to add more stuff to the same offset later

the searchable tags are sanitized as all non alphanumeric characters(excluding _) `[^a-zA-Z0-9_]+` are removed

### inverted index
passing tags a,b,c will create postings lists in the namespace
a.postings, b.postings and c.postings, later you can query only specific tags with /query

## MODIFY

```
_, err = r.Set(&AppendInput{
	ModifyPayload: []*Modify{{
		Namespace: ns,
		Offset:    off,
		Pos:       1,
		Data:      []byte("zxcv"),
	}},
})
```

inplace modifies position, for example if we want to replace 'abc'
with 'szze' in the blob we appended at offset 0, we modify rochefort
offset 0 with 'zz' from position 1
If you pass Pos: -1 it will append to the previous end of the blob

in AppendInput you can mix modify and append commands

## GET/MULTI GET

```
fetched, err := r.Get(&GetInput{
	GetPayload: []*Get{{
		Namespace: "example",
		Offset:    offset1,
	}, {
		Namespace: "example,
		Offset:    offset12,
	}},
})

```

output is GetOutput which is just array of arrays of byte, so fetched[0] is array of bytes holding the first blob and fetched[1] is the second blob

## NAMESPACE
you can also pass "namespace" parameter and this will create different directories per namespace, for example

```
namespace: events_from_20171111 
namespace: events_from_20171112
```
will crete {root_directory}/events_from_20171111/... and {root_directory}/events_from_20171112/...

and then you simply delete the directories you don't need (after closing them)

## CLOSE/DELETE
Closes a namespace so it can be deleted (or you can directly delete it with DELETE)

## STORAGE FORMAT

```
header is 16 bytes
D: data length: 4 bytes
R: reserved: 8 bytes
A: allocSize: 4 bytes
C: crc32(length, time): 4 bytes
V: the stored value

DDDDRRRRRRRRAAAACCCCVVVVVVVVVVVVVVVVVVVV...DDDDRRRRRRRRAAAACCCCVVVVVV....

```

as you can see the value is not included in the checksum, I am
checking only the header as my usecase is quite ok with
missing/corrupting the data itself, but it is not ok if corrupted
header makes us allocate 10gb in `output := make([]byte, dataLen)`


## SCAN

scans the file

```
$ curl http://localhost:8000/scan?namespace=someStoragePrefix > dump.txt
```

the format is
[len 4 bytes(little endian)][offset 8 bytes little endian)]data...[len][offset]data

## SEARCH

you can search all tagged blobs, the dsl is fairly simple, post/get json blob to  /query

* basic tag query

```
{"tag":"xyz"}
```

* basic OR query

```
{"or": [... subqueries ...]}
```


* basic AND query

```
{"and": [... subqueries ...]}
```


example:

```
curl -XGET -d '{"and":[{"tag":"c"},{"or":[{"tag":"b"},{"tag":"c"}]}]}' 'http://localhost:8000/query'
```

it spits out the output in same format as /scan, so the result of the query can be very big
but it is streamed


## LICENSE

MIT


## naming rochefort

[Rochefort Trappistes 10](https://www.ratebeer.com/beer/rochefort-trappistes-10/2360/) is my favorite beer and I was drinking it
while doing the initial implementation at sunday night


## losing data + NIH
You can lose data on crash and there is no replication, so you have
to orchestrate that yourself doing double writes or something.

The super simple architecture allows for all kinds of hacks to do
backups/replication/sharding but you have to do those yourself.

My usecase is ok with losing some data, and we dont have money to pay
for kafka+zk+monitoring(kafka,zk), nor time to learn how to optimize
it for our quite big write and very big multi-read load.

Keep in mind that there is some not-invented-here syndrome involved
into making it, but I use the service in production and it works very
nice :)

### non atomic modify
there is race between reading and modification from the client prespective



## TODO

* travis-ci
* perl client
* make c client that can be used from ruby/perl
* javadoc for the java client
* publish the java client on maven central

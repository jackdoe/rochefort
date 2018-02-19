# rochefort - poor man's kafka 

### PUSH DATA, GET FILE OFFSET; no shenanigans
#### (if you can afford to lose data and do your own replication)

* **disk write speed** storage service that returns offsets to stored values
* if you are ok with losing some data (does not fsync on write)
* supports: **append, get, multiget, close**
* clients: [go](https://github.com/jackdoe/go-rochefort-client), [java](https://github.com/jackdoe/rochefort/tree/master/clients/java), [javascript](https://github.com/jackdoe/rochefort/tree/master/clients/js), [ruby](https://github.com/jackdoe/rochefort/tree/master/clients/ruby), curl

---

## run in docker

run with docker: jackdoe/rochefort:0.3

```
docker run -e BUCKETS="10" \
           -e BIND=":8001" \
           -e ROOT="/tmp/rochefort" \
           -p 8001:8001 \
           jackdoe/rochefort:0.3
```

### parameters
* buckets: number of filers per namespace
* root: root directory, files will be created at `root/namespace||default/append.%d.raw`
* bind: address to bind to

dont forget to mount persisted root directory

## compile from source

```
$ go run main.go -buckets 10 -bind :8001 -root /tmp
2018/02/10 12:06:21 starting http server on :8001
....

```

## STORE

method post /append?id=some_identifier returns `{"offset":3659174697238528,"file":"/tmp/append.3.raw"}`

the offset encodes the bucket and the actual offset, `bucket << 50 | offset`
since java doesnot have unsigned longs, we can have at most 8191 buckets (13 bits)
(otherwise we would've had 16383 buckets or 14 bits)

Since the offset contains the bucket as well you increase the number of buckets, but never decrease them

```
$ curl -XPOST -d 'some text' 'http://localhost:8001/append?id=some_identifier'
{"offset":14636698788954112,"file":"/tmp/append.3.raw"}

$ curl -XPOST -d 'some other data in same identifier' 'http://localhost:8001/append?id=some_identifier'
{"offset":14636698788954137,"file":"/tmp/append.3.raw"}
```

## GET

method get /get?offset=14636698788954137 returns the data stored

```
$ curl 'http://localhost:8001/get?offset=14636698788954137
some other data in same identifier
```

## MULTIGET
method  post /getMulti the post data is binary 8 bytes per offset (LittleEndian), it reads until EOF
so we just ask 14636698788954137,14636698788954112,14636698788954137
```
#   \x19\x00\x00\x00\x00\x00\x34\x00 (14636698788954137 in little endian)
#   \x00\x00\x00\x00\x00\x00\x34\x00 (14636698788954112 in little endian)
#   \x19\x00\x00\x00\x00\x00\x34\x00 (14636698788954137 in little endian)

$ echo -n -e '\x19\x00\x00\x00\x00\x00\x34\x00\x00\x00\x00\x00\x00\x00\x34\x00\x19\x00\x00\x00\x00\x00\x34\x00' | \
  curl -X POST --data-binary @- http://localhost:8001/getMulti
	some text"some other data in same identifier	some t...


$ echo -n -e '\x19\x00\x00\x00\x00\x00\x34\x00\x00\x00\x00\x00\x00\x00\x34\x00\x19\x00\x00\x00\x00\x00\x34\x00'' | \
  curl -s -X POST --data-binary @- http://localhost:8001/getMulti | \
  hexdump 
0000000 22 00 00 00 73 6f 6d 65 20 6f 74 68 65 72 20 64
0000010 61 74 61 20 69 6e 20 73 61 6d 65 20 69 64 65 6e
0000020 74 69 66 69 65 72 09 00 00 00 73 6f 6d 65 20 74
0000030 65 78 74 22 00 00 00 73 6f 6d 65 20 6f 74 68 65
0000040 72 20 64 61 74 61 20 69 6e 20 73 61 6d 65 20 69
0000050 64 65 6e 74 69 66 69 65 72


```


output:

```
[4 bytes length (LittleEndian)][data][4 bytes length (LittleEndian)][data]
```

in case of error length is 0 and what follows is the error text

```
[4 bytes length (LittleEndian)][data][\0\0\0\0\0 (4 bytes of 0)]error text
```

the protocol is very simple, it stores the length of the item in 4 bytes:
`[len]some text[len]some other data in same identifier`

## NAMESPACE
you can also pass "namespace" parameter and this will create different directories per namespace, for example

```
?namespace=events_from_20171111 
?namespace=events_from_20171112
```
will crete {root_directory}/events_from_20171111/... and {root_directory}/events_from_20171112/...

and then you simply delete the directories you don't need (after closing them)

## CLOSE
Closes a namespace so it can be deleted

```
$ curl http://localhost:8000/close?namespace=events_from_20171112
{"success":true}
```

## STORAGE FORMAT

```
header is 16 bytes
D: data length: 4 bytes
T: current time in nanosecond: 8 bytes
C: crc32(length, time): 4 bytes
V: the stored value

DDDDTTTTTTTTCCCCVVVVVVVVVVVVVVVVVVVV...DDDDTTTTTTTTCCCCVVVVVV....

```

as you can see the value is not included in the checksum, I am
checking only the header as my usecase is quite ok with
missing/corrupting the data itself, but it is not ok if corrupted
header makes us allocate 10gb in `output := make([]byte, dataLen)`


## SCAN

scans all buckets from a namespace

```
$ curl http://localhost:8000/scan?namespace=someStoragePrefix > dump.txt
```

the format is
[len 4 bytes(little endian)][offset 8 bytes little endian)]data...[len][offset]data


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


## TODO

* multi append (one call, append many)
* perl client
* make c client that can be used from ruby/perl
* javadoc for the java client

# rochefort - poor man's kafka 

### PUSH DATA, GET FILE OFFSET; no shenanigans
#### (if you can afford to lose data and do your own replication)

* **disk write speed** storage service that returns offsets to stored values
* if you are ok with losing some data (does not fsync on write)
* supports: **append, modify, get, multiget, close**
* clients: [go](https://github.com/jackdoe/go-rochefort-client), [java](https://github.com/jackdoe/rochefort/tree/master/clients/java), [javascript](https://github.com/jackdoe/rochefort/tree/master/clients/js), [ruby](https://github.com/jackdoe/rochefort/tree/master/clients/ruby), curl

---

## run in docker

run with docker: jackdoe/rochefort:1.3

```
docker run -e BIND=":8000" \
           -e ROOT="/tmp/rochefort" \
           -p 8000:8000 \
           jackdoe/rochefort:1.3
```


## breaking change between 0.5 and 1.0

* added 4 more bytes in the header
* the -buckets parameter is gone, so everything is appended in one file per namespace

you can migrate your data by doing:

```
oldServer.scan(namespace: ns) do |len, offset, v|
  newServer.append(namespace:ns, data: v)
end
```

### parameters
* root: root directory, files will be created at `root/namespace||default/append.raw`
* bind: address to bind to (default :8000)

dont forget to mount persisted root directory


## compile from source

```
$ go run main.go -bind :8000 -root /tmp
2018/02/10 12:06:21 starting http server on :8000
....

```

## APPEND

method post /append?allocSize=0 returns `{"offset":0}`

you can pass allocSize so you can reserve space for in-place modifications later

```
$ curl -XPOST -d 'some text' 'http://localhost:8000/append?allocSize=1024'
{"offset":0}

$ curl -XPOST -d 'some other data in same identifier' 'http://localhost:8000/append'
{"offset":1044}
```

as you can see the offset for the second blob is 1024 bytes + header(20 bytes) away from the first blob

### inverted index
passing &tags=a,b,c to /append will create postings lists in the namespace
a.postings, b.postings and c.postings, later you can scan only specific tags with /scan


## MODIFY
method post /modify?offset=X&pos=Y returns `{"success":"true"}`

inplace modifies position, for example if we want to replace 'some'
with 'szze' in the blob we appended at offset 0, we modify rochefort
offset 0 with 'zz' from position 1

```
$ curl -XPOST -d 'zz' 'http://localhost:8000/modify?offset=0&pos=1'
{"success":"true"}
$ curl http://localhost:8000/get?offset=0
szze text
```

## GET

method get /get?offset=25 returns the data stored

```
$ curl 'http://localhost:8000/get?offset=25
some other data in same identifier
```

## MULTIGET
method  post /getMulti the post data is binary 8 bytes per offset (LittleEndian), it reads until EOF
so we just ask 0,29,0
```
#   \x00\x00\x00\x00\x00\x00\x00\x00
#   \x1D\x00\x00\x00\x00\x00\x00\x00 (29 in little endian)
#   \x00\x00\x00\x00\x00\x00\x00\x00 (0 in little endian)

$ echo -n -e '\x00\x00\x00\x00\x00\x00\x00\x00\x1D\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' | \
  curl -X POST --data-binary @- http://localhost:8000/getMulti
	some text"some other data in same identifier	some t...

$ echo -n -e '\x00\x00\x00\x00\x00\x00\x00\x00\x1D\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' | \
  curl -s -X POST --data-binary @- http://localhost:8002/getMulti | \
  hexdump 
0000000 09 00 00 00 73 6f 6d 65 20 74 65 78 74 22 00 00
0000010 00 73 6f 6d 65 20 6f 74 68 65 72 20 64 61 74 61
0000020 20 69 6e 20 73 61 6d 65 20 69 64 65 6e 74 69 66
0000030 69 65 72 09 00 00 00 73 6f 6d 65 20 74 65 78 74
0000040

```

output:

```
[4 bytes length (LittleEndian)][data][4 bytes length (LittleEndian)][data]
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

### inverted index
pass tags=a,b,c and you can search for blobs indexed in a OR b OR c


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
* multi append (one call, append many)
* perl client
* make c client that can be used from ruby/perl
* javadoc for the java client
* publish the java client on maven central
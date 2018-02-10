PUSH DATA, GET FILE OFFSET
---
no shenanigans

* run the service
* curl url/identifier returns offset and the file it was added to

$ go run main.go -buckets 10 -bind :8001
2018/02/09 23:26:17 openning: /tmp/append.0.raw with offset: 0
2018/02/09 23:26:17 openning: /tmp/append.1.raw with offset: 0
2018/02/09 23:26:17 openning: /tmp/append.2.raw with offset: 0
2018/02/09 23:26:17 openning: /tmp/append.3.raw with offset: 0
2018/02/09 23:26:17 openning: /tmp/append.4.raw with offset: 0
2018/02/09 23:26:17 openning: /tmp/append.5.raw with offset: 0
2018/02/09 23:26:17 openning: /tmp/append.6.raw with offset: 0
2018/02/09 23:26:17 openning: /tmp/append.7.raw with offset: 0
2018/02/09 23:26:17 openning: /tmp/append.8.raw with offset: 0
2018/02/09 23:26:17 openning: /tmp/append.9.raw with offset: 0


STORE post /append?id=some_identifier returns {"offset":0,"file":"/tmp/append.3.raw"}

$ curl -XPOST -d 'some text' 'http://localhost:8001/append?id=some_identifier'
{"offset":0,"file":"/tmp/append.3.raw"}

$ curl -XPOST -d 'some other data in same identifier' 'http://localhost:8001/append?id=some_identifier'
{"offset":13,"file":"/tmp/append.3.raw"}


GET get /get?id=some_identifier&offset=17 returns the data stored

$ curl 'http://localhost:8001/get?id=some_identifier&offset=17'
some other data in same identifier

MULTIGET post /getMulti?id=some_identifier the post data is binary 8 bytes per offset (LittleEndian), it reads until EOF

example: 13 is 0x0D in hex, so asking for offset 13, 0, 13

$ echo -n -e '\x0D\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0D\x00\x00\x00\x00\x00\x00\x00' | \
  curl -X POST --data-binary @- http://localhost:8001/getMulti?id=some_identifier
"some other data in same identifier	some text"some other data in same identifier

$ echo -n -e '\x0D\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0D\x00\x00\x00\x00\x00\x00\x00' | \
  curl -X POST --data-binary @- http://localhost:8001/getMulti?id=some_identifier | hexdump 
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   113  100    89  100    24  15429   4160 --:--:-- --:--:-- --:--:-- 17800
0000000 22 00 00 00 73 6f 6d 65 20 6f 74 68 65 72 20 64
0000010 61 74 61 20 69 6e 20 73 61 6d 65 20 69 64 65 6e
0000020 74 69 66 69 65 72 09 00 00 00 73 6f 6d 65 20 74
0000030 65 78 74 22 00 00 00 73 6f 6d 65 20 6f 74 68 65
0000040 72 20 64 61 74 61 20 69 6e 20 73 61 6d 65 20 69
0000050 64 65 6e 74 69 66 69 65 72
0000059

output:
4 bytes length
data
4 bytes length
data

in case of error length is 0 and what follows is the error text
4 bytes length
data
\00\0\0\0
error text

the format is very simple, it stores the length of the item in 4 bytes:
[len]some text[len]some other data in same identifier


you can also pass "storagePrefix" parameter and this will create different
directories per storagePrefix, for example
?storagePrefix=events_from_20171111 
?storagePrefix=events_from_20171112
and then you simply delete the directories you dont need

CLOSE closes storagePrefix so it can be deleted
$ curl http://localhost:8000/close?storagePrefix=events_from_20171112
{"success":true}


* not very safe, it just closes the file descriptors on sigterm/sigint
  use at your own risk


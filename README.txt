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

$ curl -XPOST -d 'some text' http://localhost:8001/some_identifier
{"offset":0,"file":"/tmp/append.2.raw"}

$ curl -XPOST -d 'some other data in same identifier' http://localhost:8001/some_identifier
{"offset":9,"file":"/tmp/append.2.raw"}
$ curl -XPOST -d 'zzz' http://localhost:8001/some_identifier
{"offset":43,"file":"/tmp/append.2.raw"}



* not very safe, it just closes the file descriptors on sigterm/sigint
  use at your own risk

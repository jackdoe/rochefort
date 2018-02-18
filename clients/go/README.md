# lives at (jackdoe/go-rochefort-client)[https://github.com/jackdoe/go-rochefort-client]

example

```
$ go get https://github.com/jackdoe/go-rochefort-client
```

main.go

```
package main

import (
	"github.com/jackdoe/go-rochefort-client"
	"log"
)

func main() {
	r := rochefort.NewClient("http://localhost:8002", nil)

	offset, _ := r.Append("namespace", "id123", []byte("some data"))
	fetched, _ := r.Get("namespace", offset)
	log.Printf("fetched: %s", string(fetched))
}

```

read the godoc for more info

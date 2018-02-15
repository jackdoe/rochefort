FROM golang:1.9

ARG CACHEBUST=1

RUN git clone https://github.com/jackdoe/rochefort && cd rochefort && go get github.com/dgryski/go-metro && go build . && mv rochefort /usr/bin/rochefort

CMD /usr/bin/rochefort -root ${ROOT:-/tmp/rochefort_files} -buckets ${BUCKETS:-32} -bind ${BIND:-:4500} 

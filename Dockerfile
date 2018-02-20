FROM golang:1.9

ARG CACHEBUST=1

RUN git clone https://github.com/jackdoe/rochefort clone && cd clone && git checkout tags/v0.5 && go get github.com/dgryski/go-metro && go build -o rochefort . && mv rochefort /usr/bin/rochefort && cd - && rm -rf clone

CMD /usr/bin/rochefort -root ${ROOT:-/tmp/rochefort_files} -buckets ${BUCKETS:-8} -bind ${BIND:-:4500}

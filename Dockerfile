FROM golang:1.9

ARG CACHEBUST=1

RUN git clone https://github.com/jackdoe/rochefort clone && cd clone && git checkout tags/v2.0 && go get github.com/dgryski/go-metro && go build -o rochefort . && mv rochefort /usr/bin/rochefort && cd - && rm -rf clone

CMD /usr/bin/rochefort -root ${ROOT:-/tmp/rochefort_files} -bind ${BIND:-:8000} $ROCHEFORT_OPTS

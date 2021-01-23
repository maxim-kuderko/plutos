FROM golang:1-alpine
RUN  apk add git
ADD . /go/src/github.com/maxim-kuderko/plutos

WORKDIR /go/src/github.com/maxim-kuderko/plutos

RUN  go get ./... && go install

FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=0 /go/bin/plutos .
EXPOSE 8080

CMD ["/root/plutos"]

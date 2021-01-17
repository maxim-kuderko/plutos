FROM golang:1.15-alpine

ADD . /go/src/github.com/maxim-kuderko/plutos

WORKDIR /go/src/github.com/maxim-kuderko/plutos

RUN go install

FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=0 /go/bin/plutos .
EXPOSE 8080

CMD ["/root/plutos"]

FROM golang:1-alpine
RUN  apk add git
ADD . /go/src/github.com/maxim-kuderko/plutos

WORKDIR /go/src/github.com/maxim-kuderko/plutos/cmd

RUN  go get ./... && go build -o plutos fasthttp.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=0 /go/src/github.com/maxim-kuderko/plutos/cmd/plutos .
EXPOSE 8080

CMD ["/root/plutos"]

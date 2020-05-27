FROM golang:1.14-alpine

ENV CGO_ENABLED=0

WORKDIR /go/src/github.com/cyverse-de/notification-migrator
COPY . .
RUN go build .

FROM scratch

WORKDIR /
COPY --from=0 /go/src/github.com/cyverse-de/notification-migrator/notification-migrator /bin/notification-migrator

ENTRYPOINT ["notification-migrator"]
CMD ["--help"]

# ===== Build stage =====
FROM golang:1.24 AS build

WORKDIR /go/src/practice-4
COPY go.* ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -v -a -o /go/bin/balancer ./cmd/lb
RUN CGO_ENABLED=0 GOOS=linux go build -v -a -o /go/bin/server ./cmd/stats
RUN CGO_ENABLED=0 GOOS=linux go build -v -a -o /go/bin/dbserver ./cmd/dbserver

# ===== Final minimal image =====
FROM alpine:latest

WORKDIR /opt/practice-4
COPY entry.sh /opt/practice-4/
COPY --from=build /go/bin/* /opt/practice-4/
RUN chmod +x /opt/practice-4/*
ENTRYPOINT ["/opt/practice-4/entry.sh"]
CMD ["server"]
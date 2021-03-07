FROM golang:latest as builder

WORKDIR /go-modules

COPY . ./

# Building using -mod=vendor, which will utilize the v
#RUN go get
#RUN go mod vendor
RUN CGO_ENABLED=0 GOOS=linux go build -v -mod=vendor -o gateway-bbox

#FROM alpine:3.8
FROM scratch
# We need ca certs installed otherwise we can't get https urls
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

WORKDIR /root/

COPY --from=builder /go-modules/gateway-bbox .
COPY conf.json .

CMD ["./gateway-bbox"]
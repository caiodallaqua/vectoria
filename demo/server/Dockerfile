FROM golang:1.23.6-alpine3.21 AS build
COPY . /build
WORKDIR /build
RUN go build -o server

FROM alpine:latest AS final
COPY --from=build /build/server ./
CMD ["./server"]
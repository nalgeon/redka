FROM alpine:edge AS build
RUN apk add --no-cache --update gcc g++ go make
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN make build build-cli

FROM alpine:latest
RUN apk add --no-cache redis
RUN mkdir /data
VOLUME /data
WORKDIR /data
COPY --from=build /app/build/redka /usr/local/bin/redka
COPY --from=build /app/build/redka-cli /usr/local/bin/redka-cli
HEALTHCHECK CMD redis-cli PING || exit 1
EXPOSE 6379
CMD ["redka", "-h", "0.0.0.0", "-p", "6379", "redka.db"]

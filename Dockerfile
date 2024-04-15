FROM alpine:edge AS build
RUN apk add --no-cache --allow-untrusted --update gcc g++ go make
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN make build build-cli

FROM alpine:latest
RUN mkdir /data
VOLUME /data
WORKDIR /data
COPY --from=build /app/build/redka /usr/local/bin/redka
COPY --from=build /app/build/redka-cli /usr/local/bin/redka-cli
EXPOSE 6379
CMD ["redka", "-h", "0.0.0.0", "-p", "6379", "redka.db"]

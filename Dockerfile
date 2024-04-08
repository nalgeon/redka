FROM alpine:edge AS build
RUN apk add --no-cache --update gcc g++ go make
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN make build

FROM alpine:latest
RUN mkdir /data
VOLUME /data
WORKDIR /data
COPY --from=build /app/build/redka /usr/local/bin/redka
EXPOSE 6379
CMD ["redka", "-h", "0.0.0.0"]

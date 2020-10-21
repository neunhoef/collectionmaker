FROM golang:1.15.0
ARG GOOS=linux
ARG GOARCH=amd64

WORKDIR /app
COPY cmd /app/cmd
COPY pkg /app/pkg
COPY *.go go.mod go.sum /app/

RUN CGO_ENABLED=0 go build -o /app/bin/${GOOS}/${GOARCH}/collectionmaker
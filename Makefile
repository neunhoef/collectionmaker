ifndef GOOS
	GOOS := linux
endif

ifndef GOARCH
	GOARCH := amd64
endif

BIN_FILE=bin/${GOOS}/${GOARCH}/collectionmaker

# Build binary locally.
build:
	go build -o ${BIN_FILE} -tags netgo

# Build binary without installed GO and copy it to the local system.
docker:
	docker build  --build-arg GOOS=${GOOS} --build-arg GOARCH=${GOARCH} -t collectionmaker .
	@mkdir -p ./bin/${GOOS}/${GOARCH}
	@-docker rm collectionmaker-instance
	docker create --name collectionmaker-instance collectionmaker
	docker cp collectionmaker-instance:/app/${BIN_FILE} ${BIN_FILE}
	docker rm collectionmaker-instance

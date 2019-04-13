GOCMD=go
    GOBUILD=$(GOCMD) build
    GOGET=$(GOCMD) get
    BINARY_NAME=gohep
    BINARY_UNIX=$(BINARY_NAME)_unix
    
build:
	go generate
	$(GOBUILD) -o $(BINARY_NAME) -v

	
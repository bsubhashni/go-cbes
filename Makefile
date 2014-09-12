export GOPATH=$(PWD)

all:
	@go get code.google.com/p/go.crypto/ssh
	@go get github.com/couchbaselabs/go-couchbase
	@go build
	@mkdir  bin
	@mv Go-ES-Couchbase bin/.

.PHONY: clean
clean:
	rm -rf bin



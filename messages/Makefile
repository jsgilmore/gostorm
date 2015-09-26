export PATH := $(GOPATH)/bin:$(PATH)

regenerate:
	(protoc --proto_path=$(GOPATH)/src:$(GOPATH)/src/github.com/gogo/protobuf/protobuf:. --gogofast_out=. messages.proto)

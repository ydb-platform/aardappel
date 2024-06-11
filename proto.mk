.PHONY: gen_proto
gen_proto:
	find internal/protos -iname '*.proto' | xargs -n 1 dirname | uniq | \
	  xargs -n 1 -I '{}' bash -c "protoc --go_out=. --go_opt=paths=source_relative \
	    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	    {}/*.proto"

.PHONY: fetch_protoc
fetch_protoc:
	@if protoc --version >/dev/null; \
	  then \
	    echo "protoc has already been installed" ; \
	  else \
	    echo "Installing protoc"; \
	    if test $(uname) Darwin; then \
	      brew install protobuf ; \
	    else \
	      curl -L --output protoc.zip "https://github.com/protocolbuffers/protobuf/releases/download/v25.1/protoc-25.1-linux-x86_64.zip" ; \
	      unzip protoc.zip -d $HOME/.local ; \
	      echo "I've downloaded protoc binary to $HOME/.local. You're on your own now." ; \
	    fi ; \
	fi

	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

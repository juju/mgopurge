build: generate
	gb build

release: clean generate
	gb build -f

clean:
	rm -rf bin/* pkg/* src/mgopurge/version.go

generate:
	gb generate

.PHONY: build generate release clean

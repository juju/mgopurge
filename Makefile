build: generate
	go build

# Remember to create annotated tag first. E.g. git tag -a v1.2
release: clean check-clean generate 
	go build -f

snap-release: clean generate
	# snap modifies the source tree to remove the snap directory, so we can't run 'make check-clean'
	go build -f
	cp bin/mgopurge ${SNAPCRAFT_PART_INSTALL}

clean:
	rm -rf bin/* pkg/* src/mgopurge/version.go

check-clean:
	@test -z "$$(git status -s)" || ( echo "uncommitted changes" ; false )

generate: dependencies
	go generate

.PHONY: build dependencies generate release clean check-clean

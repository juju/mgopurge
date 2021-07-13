GB := ${GOPATH}/bin/gb

build: generate
	${GB} build

# Remember to create annotated tag first. E.g. git tag -a v1.2
release: clean check-clean generate
	${GB} build -f

# Make sure that gb is available first
dependencies:
	go get github.com/constabulary/gb/...

snap-release: clean generate dependencies
	# snap modifies the source tree to remove the snap directory, so we can't run 'make check-clean'
	${GB} build -f
	cp bin/mgopurge ${SNAPCRAFT_PART_INSTALL}

clean:
	rm -rf bin/* pkg/* src/mgopurge/version.go

check-clean:
	@test -z "$$(git status -s)" || ( echo "uncommitted changes" ; false )

generate: dependencies
	${GB} generate

.PHONY: build dependencies generate release clean check-clean

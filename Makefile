build: generate
	gb build

# Remember to create annotated tag first. E.g. git tag -a v1.2
release: clean check-clean generate
	gb build -f

snap-release: clean generate
	gb build -f
	cp bin/mgopurge ${SNAPCRAFT_PART_INSTALL}

clean:
	rm -rf bin/* pkg/* src/mgopurge/version.go

check-clean:
	@test -z "$$(git status -s)" || ( echo "uncommitted changes" ; false )

generate:
	gb generate

.PHONY: build generate release clean check-clean

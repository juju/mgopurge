build: generate
	gb build

release: clean check-clean generate
	gb build -f

clean:
	rm -rf bin/* pkg/* src/mgopurge/version.go

check-clean:
	@test -z "$$(git status -s)" || ( echo "uncommitted changes" ; false )

generate:
	gb generate

.PHONY: build generate release clean check-clean

.PHONY: fmt-check check-mod vet test test-race build check

export GOWORK=off
export GOTOOLCHAIN=auto

fmt-check:
	@test -z "$$(gofmt -l .)"

check-mod:
	go mod tidy -diff
	go mod verify
	@replacements="$$(go list -m -f '{{if .Replace}}{{.Path}}{{end}}' all)" && test -z "$$replacements"
	@test "$$(go list -m -f '{{.GoVersion}}')" = "1.26.8"
	@test "$$(go list -m -f '{{.Version}}' github.com/mattsp1290/eino-agent)" = "v0.3.4-0.20260910151824-b77c7e64e09d"
	@test "$$(go list -m -f '{{.Version}}' github.com/mattsp1290/eino-providers)" = "v0.0.0-20260910030140-fb8ed3137d58"
	@test "$$(go list -m -f '{{.Version}}' github.com/mattsp1290/opencode-auth-go)" = "v0.0.0-20260908211055-a3f44cca7a18"
	@test "$$(go list -m -f '{{.Version}}' github.com/cloudwego/eino)" = "v0.8.13"
	@test "$$(go list -m -f '{{.Version}}' github.com/slack-go/slack)" = "v0.29.0"
	@test "$$(go list -m -f '{{.Version}}' github.com/bwmarrin/discordgo)" = "v0.29.0"
	@test "$$(go list -m -f '{{.Version}}' modernc.org/sqlite)" = "v1.54.0"
	@deps="$$(go list -deps ./cmd/eino-channels)" && case "$$deps" in *eino-tui/internal*) exit 1;; esac

vet:
	go vet ./...

test:
	go test ./...

test-race:
	go test -race ./...

build:
	go build ./cmd/eino-channels

check: fmt-check check-mod vet test test-race build

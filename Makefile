include make/*.mk

tools:
	@which podman
	@podman version
	@which redis-cli
	@redis-cli --version
	@which go
	@go version


deps:
	go mod download
	go mod verify
	go mod tidy


run: start

start:
	go run main.go

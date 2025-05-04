# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands
- Always use Nix develop environment: `nix develop` or `nix develop -c <command>`
- Install dependencies: `go mod download`
- Run server: `go run cmd/server/server_main.go run [flags]`
- Build CLI: `go build -o datas3t-cli ./cmd/datas3t-cli`

## Test Commands
- Run all tests: `go test ./...`
- Run specific test: `go test ./path/to/package -run=^TestName$`
- Run short tests only: `go test -short ./...`

## Lint Commands
- Go code verification: `go vet ./...`

## Code Style Guidelines
- **Imports**: Standard lib first, third-party second, project imports last
- **Formatting**: Use standard Go formatting (gofmt)
- **Types**: Strong typing with descriptive custom types for domain objects
- **Naming**: CamelCase for exported, lowerCamelCase for unexported identifiers
- **Error Handling**: Separate operations from error checks; don't use inline error checking
- **Testing**: Use table-driven tests with descriptive names and testify assertions
- **Documentation**: Comment exported functions following Go standards

## Project-Specific Rules
- **API Consistency**: When modifying server endpoints, update client, CLI, and tests
- **Database**: Create new migration files for schema changes, never modify existing ones
- **Cucumber Tests**: Follow single Given-When-Then flow in each scenario
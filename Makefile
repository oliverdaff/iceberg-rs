
# Build project
build:
	cargo build

# Run tests
test:
	cargo test

check-clippy:
	cargo clippy

check-fmt:
	cargo fmt --check

build-all-test:
	cargo test --no-run

run-all-unit-test:
	cargo test --lib

run-all-doc-test:
	cargo test --doc

# Run validation checks
validate: check-clippy check-fmt test

install-clippy:
	rustup component add clippy
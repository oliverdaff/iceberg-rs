
# Build project
build:
	cargo build

# Run tests
test:
	cargo test --locked

check-clippy: install-clippy
	cargo clippy --locked -- -D warnings

check-fmt:
	cargo fmt --check

build-all-test:
	cargo test --no-run --locked

run-all-unit-test:
	cargo test --lib --locked

run-all-doc-test:
	cargo test --doc --locked

# Run validation checks
validate: check-clippy check-fmt test

install-clippy:
	rustup component add clippy
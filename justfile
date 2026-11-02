# The emulator runs under its own compose project, so it never collides with other stands.
compose := "docker compose -p faststream-gcp-pubsub"
emulator_port := env("PUBSUB_EMULATOR_PORT", "8085")

default:
    @just --list

lint:
    uv run ruff check
    uv run ruff format --check
    uv run mypy
    uv run pytest --collect-only -q -m "" -p tests.unused_fixtures

# the workflows: actionlint (with shellcheck over the `run:` scripts) and zizmor
lint-workflows:
    uv run actionlint
    uv run zizmor .github

fmt:
    uv run ruff format
    uv run ruff check --fix

test *args:
    uv run pytest {{ args }}

emulator-up:
    {{ compose }} up -d --wait pubsub

emulator-down:
    {{ compose }} down

test-gcp *args:
    PUBSUB_EMULATOR_HOST=localhost:{{ emulator_port }} uv run pytest -m gcp {{ args }}

check: lint test

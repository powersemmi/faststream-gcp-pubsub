# Contributing

Thank you for your interest in `faststream-gcp-pubsub`. Issues and pull requests are welcome.

## Set up

You need [uv](https://docs.astral.sh/uv/) and [just](https://just.systems/). Python 3.10 to 3.14 is supported.

```bash
git clone https://github.com/powersemmi/faststream-gcp-pubsub
cd faststream-gcp-pubsub
uv sync
```

Install the git hooks (ruff, mypy, codespell, actionlint, zizmor, commit message check):

```bash
uvx pre-commit install --install-hooks
uvx pre-commit install --hook-type commit-msg
```

## Lint and type checks

```bash
just lint             # ruff check, ruff format --check, mypy (strict), unused test fixtures
just lint-workflows   # actionlint and zizmor over .github
just fmt              # apply ruff formatting and safe fixes
```

## Tests

The in-process suite needs no external services:

```bash
just test
```

Live tests run against the Pub/Sub emulator in Docker:

```bash
just emulator-up
just test-gcp
just emulator-down
```

Every Python example of the documentation is a file under `docs/docs_src` that a test under
`tests/docs` runs. A page includes the file; the README and the docstrings copy an excerpt of
it verbatim, and `tests/docs/test_examples_are_tested.py` checks that they still match.

`just check` runs lint and the in-process suite. A change that touches the transport should also pass `just test-gcp`.

## Commits and pull requests

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/), for example `fix: nack a message when the handler is cancelled` or `feat: support ordering keys`.

Keep one logical change per pull request. Add tests for new behaviour, update the documentation when behaviour changes, and fill in the pull request template.

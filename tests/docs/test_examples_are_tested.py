import ast
import re
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
SNIPPETS = ROOT / "docs" / "docs_src"
SNIPPETS_PACKAGE = "docs.docs_src"
PAGES = ROOT / "docs" / "docs"
PACKAGE = ROOT / "faststream_gcp_pubsub"

FENCE = re.compile(
    r"^(?P<indent> *)```(?P<lang>\S*)[^\n]*\n(?P<body>.*?)^(?P=indent)```",
    re.MULTILINE | re.DOTALL,
)
SNIPPET_MARKER = re.compile(r"^\s*# --8<-- \[(start|end):\w+\]\s*$")


def test_every_snippet_is_imported_by_a_test() -> None:
    # a new snippet gets a test that imports it
    assert _untested_snippets() == set()


def _python_fences(path: Path) -> list[tuple[int, str]]:
    text = path.read_text(encoding="utf-8")
    return [
        (text.count("\n", 0, match.start()) + 1, textwrap.dedent(match["body"]))
        for match in FENCE.finditer(text)
        if match["lang"] in {"python", "py"}
    ]


def _page_examples() -> list[tuple[str, str]]:
    return [
        (f"{page.relative_to(ROOT)}:{line}", body)
        for page in sorted(PAGES.rglob("*.md"))
        for line, body in _python_fences(page)
    ]


def _readme_examples() -> list[tuple[str, str]]:
    return [
        (f"README.md:{line}", body) for line, body in _python_fences(ROOT / "README.md")
    ]


def _docstring_examples() -> list[tuple[str, str]]:
    examples: list[tuple[str, str]] = []
    for module in sorted(PACKAGE.rglob("*.py")):
        for node in ast.walk(ast.parse(module.read_text(encoding="utf-8"))):
            if not isinstance(
                node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef
            ):
                continue
            docstring = ast.get_docstring(node)
            if docstring is None:
                continue
            where = f"{module.relative_to(ROOT)}:{node.name}"
            examples.extend((where, example) for example in _doctest_blocks(docstring))
    return examples


def _doctest_blocks(docstring: str) -> list[str]:
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in textwrap.dedent(docstring).splitlines():
        stripped = line.strip()
        if stripped.startswith((">>> ", "... ")) or stripped in {">>>", "..."}:
            current.append(stripped[4:])
        elif current:
            blocks.append(current)
            current = []
    if current:
        blocks.append(current)
    return ["\n".join(block) + "\n" for block in blocks]


@pytest.mark.parametrize(
    "body", [pytest.param(body, id=where) for where, body in _page_examples()]
)
def test_every_python_example_of_the_pages_is_an_included_file(body: str) -> None:
    # the page shows a file, so the code a reader copies is the code a test runs
    lines = [line for line in body.splitlines() if line.strip()]
    assert lines
    assert all(line.startswith('--8<-- "') for line in lines), body


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(body, id=where)
        for where, body in [*_readme_examples(), *_docstring_examples()]
    ],
)
def test_every_inline_example_is_an_excerpt_of_a_tested_file(body: str) -> None:
    # the README and the docstrings render where a snippet cannot be included, so each
    # example is copied verbatim from a snippet or a docs test
    assert any(_contains(source, body) for source in _excerpt_sources()), body


def _excerpt_sources() -> list[list[str]]:
    return [
        [
            line
            for line in path.read_text(encoding="utf-8").splitlines()
            if not SNIPPET_MARKER.match(line)
        ]
        for root in (SNIPPETS, ROOT / "tests" / "docs")
        for path in sorted(root.rglob("*.py"))
    ]


def _contains(source: list[str], excerpt: str) -> bool:
    wanted = excerpt.strip("\n").splitlines()
    for start in range(len(source) - len(wanted) + 1):
        window = "\n".join(source[start : start + len(wanted)])
        if textwrap.dedent(window).splitlines() == wanted:
            return True
    return False


def _untested_snippets() -> set[str]:
    snippets = {
        _module_name(path): path
        for path in SNIPPETS.rglob("*.py")
        if path.name != "__init__.py"
    }

    reached: set[str] = set()
    queue = list((ROOT / "tests").rglob("*.py"))
    while queue:
        for name in _referenced_modules(queue.pop()):
            if name in snippets and name not in reached:
                reached.add(name)
                # a snippet that another tested snippet imports runs with it
                queue.append(snippets[name])

    return {
        path.relative_to(SNIPPETS).as_posix()
        for name, path in snippets.items()
        if name not in reached
    }


def _referenced_modules(path: Path) -> set[str]:
    package = _module_name(path).rsplit(".", 1)[0].split(".")
    names: set[str] = set()

    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.ImportFrom):
            base = package[: len(package) - node.level + 1] if node.level else []
            module = ".".join([*base, *([node.module] if node.module else [])])
            names.add(module)
            names.update(f"{module}.{alias.name}" for alias in node.names)

        elif isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)

        # "docs.docs_src.home.service:app", as a CLI test spells it
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and node.value.startswith(SNIPPETS_PACKAGE)
        ):
            names.add(node.value.partition(":")[0])

    return names


def _module_name(path: Path) -> str:
    return ".".join(path.relative_to(ROOT).with_suffix("").parts)

"""Reading and writing SQL text, for prytt's ``spark.sql`` rewrites and DDL.

Interface:

- ``ident(name) -> str``: ``name`` as a backtick-quoted identifier.
- ``literal(value) -> str``: ``value`` as a single-quoted string literal
  (``bool`` becomes ``'true'`` / ``'false'``).
- ``mask(sql) -> str``: ``sql`` with the same length, but comments and string
  literal contents blanked to spaces and backtick identifier contents to
  ``x``, so keywords and ``;`` found in the result are real SQL tokens.
- ``parse_name(text) -> list[str] | None``: the parts of a whole multi-part
  name such as ``db.tbl`` or `` `db`.`tbl` `` (backticks removed), or
  ``None`` if ``text`` is not one.
- ``table_refs(sql) -> [(start, end, parts)]``: each multi-part name in a
  table position: right after ``FROM``, ``JOIN``, ``TABLE``, ``INTO``,
  ``UPDATE`` or ``USING``, or after a ``,`` in a ``FROM`` list. A heuristic,
  not a parser: column references (``alias.col``), struct fields, numbers
  and names inside comments or strings are never returned.
"""

import bisect
import re

_PART = r"(?:`(?:[^`]|``)*`|\w+)"
_NAME = re.compile(rf"(?<![\w.`]){_PART}(?:\s*\.\s*{_PART})*")
_PART_RE = re.compile(_PART)
_TOKEN = re.compile(r"`[^`]*`|\w+|[(),;]")
_TABLE_KEYWORDS = {"FROM", "JOIN", "TABLE", "INTO", "UPDATE", "USING"}
# Clause keywords that end the search back from a ``,`` for its ``FROM``.
_CLAUSE_KEYWORDS = {
    "SELECT",
    "WHERE",
    "GROUP",
    "ORDER",
    "HAVING",
    "ON",
    "LIMIT",
    "SET",
    "VALUES",
    "WINDOW",
    "BY",
}


def ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def literal(value) -> str:
    if isinstance(value, bool):
        value = str(value).lower()
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def mask(sql: str) -> str:
    out = list(sql)
    n = len(sql)

    def blank(start, end, char=" "):
        for k in range(start, end):
            if out[k] != "\n":
                out[k] = char

    i = 0
    while i < n:
        if sql.startswith("--", i):
            end = sql.find("\n", i)
            end = n if end == -1 else end
            blank(i, end)
        elif sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            end = n if end == -1 else end + 2
            blank(i, end)
        elif sql[i] in "'\"`":
            quote = sql[i]
            end = i + 1
            while end < n:
                if quote != "`" and sql[end] == "\\":
                    end += 2
                elif sql[end] == quote and sql.startswith(quote * 2, end):
                    end += 2  # doubled quote inside the literal
                elif sql[end] == quote:
                    break
                else:
                    end += 1
            end = min(end, n)
            blank(i + 1, end, "x" if quote == "`" else " ")
            end += 1
        else:
            end = i + 1
        i = end
    return "".join(out)


def _unquote(part: str) -> str:
    return part[1:-1].replace("``", "`") if part.startswith("`") else part


def parse_name(text: str) -> list | None:
    text = text.strip()
    if not _NAME.fullmatch(text):
        return None
    return [_unquote(p) for p in _PART_RE.findall(text)]


def _in_table_position(tokens: list, index: int) -> bool:
    """True if the name right after ``tokens[:index]`` is a table name."""
    if not index:
        return False
    previous = tokens[index - 1].upper()
    if previous in _TABLE_KEYWORDS:
        return True
    if previous != ",":
        return False
    depth = 0
    for token in reversed(tokens[: index - 1]):
        word = token.upper()
        if token == ")":
            depth += 1
        elif token == "(":
            if not depth:
                return False  # inside a call's arguments
            depth -= 1
        elif depth:
            continue
        elif word == "FROM":
            return True
        elif word in _CLAUSE_KEYWORDS or token == ";":
            return False
    return False


def table_refs(sql: str) -> list:
    masked = mask(sql)
    tokens = [(m.start(), m.group().upper()) for m in _TOKEN.finditer(masked)]
    starts = [start for start, _ in tokens]
    words = [word for _, word in tokens]
    refs = []
    for m in _NAME.finditer(masked):
        parts = _PART_RE.findall(sql[m.start() : m.end()])
        if len(parts) < 2 or any(p.isdigit() for p in parts):
            continue
        index = bisect.bisect_left(starts, m.start())
        if _in_table_position(words, index):
            refs.append((m.start(), m.end(), [_unquote(p) for p in parts]))
    return refs

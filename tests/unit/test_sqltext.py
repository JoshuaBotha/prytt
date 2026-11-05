"""``sqltext.statements``: SQL split into statements of tokens, no Spark needed."""

import pytest

from prytt.sqltext import Token, statements


def texts(sql):
    return [[token.text for token in statement] for statement in statements(sql)]


def test_tokens_have_a_kind_value_and_keyword_word():
    [tokens] = statements("INSERT INTO `my db`.T (a, b) VALUES ('x')")

    assert tokens == [
        Token("name", "INSERT", ["INSERT"], "INSERT"),
        Token("name", "INTO", ["INTO"], "INTO"),
        Token("name", "`my db`.T", ["my db", "T"], ""),
        Token("punct", "(", "(", ""),
        Token("name", "a", ["a"], "A"),
        Token("punct", ",", ",", ""),
        Token("name", "b", ["b"], "B"),
        Token("punct", ")", ")", ""),
        Token("name", "VALUES", ["VALUES"], "VALUES"),
        Token("punct", "(", "(", ""),
        Token("string", "'x'", "x", ""),
        Token("punct", ")", ")", ""),
    ]


def test_multi_part_names_with_spaces_around_dots_are_one_token():
    [tokens] = statements("SELECT * FROM db . `t``x`")

    assert tokens[-1].value == ["db", "t`x"]
    assert tokens[-1].word == ""


def test_one_part_backquoted_name_is_not_a_keyword():
    [tokens] = statements("SELECT `from`")

    assert tokens[1] == Token("name", "`from`", ["from"], "")


def test_other_punctuation_and_operators_are_dropped():
    assert texts("SELECT a + 1 * 2 FROM t WHERE a >= 3") == [
        ["SELECT", "a", "1", "2", "FROM", "t", "WHERE", "a", "3"]
    ]


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("-- INSERT INTO t\nSELECT 1", [["SELECT", "1"]]),
        ("SELECT /* INSERT INTO t */ 1", [["SELECT", "1"]]),
        ("SELECT 1 -- trailing; comment", [["SELECT", "1"]]),
        ("/* unclosed comment; DROP TABLE t", []),
        ("SELECT 1 /* a\n; b */ FROM t", [["SELECT", "1", "FROM", "t"]]),
    ],
)
def test_comments_are_skipped(sql, expected):
    assert texts(sql) == expected


@pytest.mark.parametrize(
    ("literal", "value"),
    [
        ("'plain'", "plain"),
        ("'it''s'", "it's"),
        ("'it\\'s'", "it's"),
        ("'back\\\\slash'", "back\\slash"),
        ('"double ""quoted"""', 'double "quoted"'),
        ("'a; -- b /* c */'", "a; -- b /* c */"),
        ("''", ""),
    ],
)
def test_string_literals_are_unescaped(literal, value):
    [tokens] = statements(f"SELECT {literal}")

    assert tokens[1] == Token("string", literal, value, "")


def test_statements_split_on_semicolons():
    assert texts("USE DATABASE d; INSERT INTO t SELECT 1;;  ; DROP TABLE t;") == [
        ["USE", "DATABASE", "d"],
        ["INSERT", "INTO", "t", "SELECT", "1"],
        ["DROP", "TABLE", "t"],
    ]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 'a;b' FROM t",
        'SELECT "a;b" FROM t',
        "SELECT `a;b` FROM t",
        "SELECT 1 -- ; DROP TABLE x\nFROM t",
        "SELECT 1 /* ; DROP TABLE x */ FROM t",
    ],
)
def test_semicolons_inside_strings_identifiers_and_comments_do_not_split(sql):
    [tokens] = statements(sql)

    assert tokens[0].word == "SELECT"
    assert tokens[-1].text == "t"
    assert "DROP" not in [token.word for token in tokens]


def test_empty_sql_has_no_statements():
    assert statements("") == []
    assert statements("  ;\n; -- only a comment") == []

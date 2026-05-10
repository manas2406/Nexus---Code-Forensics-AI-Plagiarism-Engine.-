"""AST-based token extraction for C++ source code using tree-sitter.

Extracts node types only — variable names, literals, and identifiers are
discarded, making the engine invariant to renaming.
"""

from __future__ import annotations

import sys

import tree_sitter
import tree_sitter_cpp

_CPP_LANGUAGE = tree_sitter.Language(tree_sitter_cpp.language())
_PARSER = tree_sitter.Parser(_CPP_LANGUAGE)


def extract_tokens(source_code: str) -> list[str]:
    """Parse C++ source and return a list of tree-sitter node type strings.

    Traverses the AST depth-first, collecting node types.  Identifier,
    literal, and other naming nodes are implicitly included by type
    (e.g. ``"identifier"``, ``"number_literal"``) but their *text* is
    never recorded — only the structural node type matters.

    If the parser encounters an ``ERROR`` node, a warning is logged to
    stderr and that entire subtree is skipped.  If the file is completely
    unparseable, an empty list is returned.

    Args:
        source_code: Raw C++ source as a string.

    Returns:
        A list of node-type strings in depth-first traversal order.
    """
    if not source_code:
        return []

    tree = _PARSER.parse(source_code.encode("utf-8"))
    root = tree.root_node

    tokens: list[str] = []
    had_errors = _walk(root, tokens)  # noqa: F841  — used in pipeline
    return tokens


def extract_tokens_with_errors(source_code: str) -> tuple[list[str], bool]:
    """Like ``extract_tokens`` but also returns whether ERROR nodes were seen.

    Args:
        source_code: Raw C++ source as a string.

    Returns:
        A tuple of ``(tokens, had_errors)``.
    """
    if not source_code:
        return [], False

    tree = _PARSER.parse(source_code.encode("utf-8"))
    root = tree.root_node

    tokens: list[str] = []
    had_errors = _walk(root, tokens)
    return tokens, had_errors


def _walk(node: tree_sitter.Node, tokens: list[str]) -> bool:
    """Recursively traverse the AST, appending node types to *tokens*.

    Args:
        node: The current tree-sitter node.
        tokens: Accumulator list that is mutated in place.

    Returns:
        ``True`` if any ``ERROR`` node was encountered in this subtree.
    """
    had_errors = False

    if node.is_error:
        print(
            f"[ast_engine] WARN: ERROR node at byte {node.start_byte}",
            file=sys.stderr,
        )
        return True  # skip this subtree

    tokens.append(node.type)

    for child in node.children:
        if _walk(child, tokens):
            had_errors = True

    return had_errors

"""Utilities with improved style, correct comparisons and efficient loops.

This module fixes common issues: unused variables, wrong comparisons
(use `is None`), and suboptimal loops by using comprehensions and
generator expressions. Functions are type hinted and include simple
assert-based sanity checks when run as a script.
"""

from __future__ import annotations

from typing import Iterable, List, Optional


def sum_evens(numbers: Iterable[int]) -> int:
    """Return the sum of even numbers from the iterable.

    Uses a generator expression for memory efficiency.
    """
    return sum(x for x in numbers if x % 2 == 0)


def safe_divide(a: float, b: float) -> Optional[float]:
    """Return a / b or None when dividing by zero.

    Use explicit equality to check zero and return None to signal
    an invalid operation instead of raising.
    """
    if b == 0:
        return None
    return a / b


def find_first_match(items: List[str], target: str) -> Optional[int]:
    """Return the index of the first occurrence of target, or None."""
    for index, value in enumerate(items):
        if value == target:
            return index
    return None


def normalize_text(s: str) -> str:
    """Normalize a string for comparison: strip and lower-case."""
    return s.strip().lower()


if __name__ == "__main__":
    # Quick sanity checks
    assert sum_evens([1, 2, 3, 4]) == 6
    assert safe_divide(10, 2) == 5
    assert safe_divide(1, 0) is None
    assert find_first_match(["a", "b", "c"], "b") == 1
    assert find_first_match(["x"], "y") is None
    assert normalize_text("  Hello ") == "hello"
    print("example.py: self-checks passed")

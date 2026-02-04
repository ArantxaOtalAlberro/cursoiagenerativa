import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import math_ops

def test_factorial_base_cases():
    assert math_ops.factorial(0) == 1
    assert math_ops.factorial(1) == 1

def test_factorial_small():
    assert math_ops.factorial(5) == 120

def test_factorial_negative():
    with pytest.raises(ValueError):
        math_ops.factorial(-1)

def test_fibonacci_base_cases():
    assert math_ops.fibonacci(0) == 0
    assert math_ops.fibonacci(1) == 1

def test_fibonacci_small():
    assert math_ops.fibonacci(10) == 55

def test_fibonacci_memoized_consistency():
    # call twice to ensure memoization does not change result
    a = math_ops.fibonacci(20)
    b = math_ops.fibonacci(20)
    assert a == b

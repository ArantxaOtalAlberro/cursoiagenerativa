from functools import lru_cache

def factorial(n: int) -> int:
    """Calcula el factorial de un número entero no negativo."""
    if n < 0:
        raise ValueError("El factorial no está definido para números negativos")
    if n == 0 or n == 1:
        return 1
    return n * factorial(n - 1)


@lru_cache(maxsize=None)
def fibonacci(n: int) -> int:
    """Calcula el n-ésimo número de Fibonacci."""
    if n <= 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fibonacci(n - 1) + fibonacci(n - 2)

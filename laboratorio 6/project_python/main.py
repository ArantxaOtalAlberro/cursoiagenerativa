#!/usr/bin/env python3
from utils import greet_user
from math_ops import factorial, fibonacci

def main():
    user_name = input("Introduce tu nombre: ")
    greet_user(user_name)
    try:
        number = int(input("Introduce un número para calcular factorial y Fibonacci: "))
    except ValueError:
        print("Por favor, introduce un número entero válido.")
        return
    try:
        print(f"Factorial de {number}: {factorial(number)}")
    except ValueError as e:
        print("Error:", e)
    print(f"Fibonacci de {number}: {fibonacci(number)}")

if __name__ == "__main__":
    main()

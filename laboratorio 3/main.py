from utils import greet_user, read_csv_to_dict
from math_ops import factorial, fibonacci


def main():
    name = input("Nombre: ")
    greet_user(name)

    number = input("Numero: ")

    # No hay validación de entrada
    number = int(number)

    print("Factorial:", factorial(number))
    print("Fibonacci:", fibonacci(number))


if __name__ == "__main__":
    main()

    # Ejemplo de uso de la función que lee CSV
    try:
        data_list = read_csv_to_dict("sample.csv")
        print("CSV como lista:", data_list)
        data_by_id = read_csv_to_dict("sample.csv", key="id")
        print("CSV indexado por 'id':", data_by_id)
    except Exception as e:
        print("Error leyendo sample.csv:", e)

from typing import Dict, List, Iterator, Union, Optional, Any, Tuple
import csv
import os


def greet_user(name: str) -> None:
    print("Hola " + name)


def read_csv_to_dict(
    path: str,
    key: Optional[str] = None,
    delimiter: str = ',',
    encoding: str = 'utf-8',
    allow_duplicates: bool = False,
    stream: bool = False,
    filter_column: Optional[str] = None,
    filter_value: Optional[str] = None,
) -> Union[List[Dict[str, str]], Dict[str, Union[Dict[str, str], List[Dict[str, str]]]], Iterator[Dict[str, str]], Iterator[Tuple[str, Dict[str, str]]]]:
    """Lee un CSV y devuelve filas como lista o diccionario indexado por columna.

    Mejoras respecto a la versión anterior:
    - Añade anotaciones de tipos.
    - Maneja explícitamente excepciones de archivo y CSV.
    - Opción `stream=True` para obtener un iterador que no carga todo en memoria.
    - Opción `allow_duplicates=True` para agrupar filas que comparten la misma clave.

    Comportamiento:
    - Si `key` es None y `stream` es False: devuelve `List[Dict[str,str]]`.
    - Si `key` es None y `stream` es True: devuelve `Iterator[Dict[str,str]]`.
    - Si `key` es proporcionado y `stream` es False: devuelve `Dict[key->row]` (o `key->List[row]` si `allow_duplicates`).
    - Si `key` es proporcionado y `stream` es True: devuelve `Iterator[(key_value, row)]`.

    Args:
        path: Ruta al CSV.
        key: Nombre de la columna que se usará como clave (opcional).
        delimiter: Separador de campos.
        encoding: Codificación del archivo.
        allow_duplicates: Si True, agrupa filas con la misma clave en listas.
        stream: Si True, devuelve un iterador en lugar de cargar todo en memoria.
        filter_column: Si se proporciona junto con `filter_value`, sólo se devuelven filas
            cuyo valor en `filter_column` sea igual a `filter_value`.
        filter_value: Valor a filtrar en `filter_column`.

    Returns:
        Lista, diccionario o iterador según los parámetros.

    Raises:
        FileNotFoundError: Si `path` no existe.
        PermissionError: Si no hay permisos para leer el archivo.
        ValueError: Si el CSV no contiene cabecera.
        KeyError: Si `key` no está en las cabeceras.
        csv.Error: Para errores de parsing del CSV.
    """

    def _iter_rows() -> Iterator[Dict[str, str]]:
        f = open(path, encoding=encoding, newline='')
        try:
            reader = csv.DictReader(f, delimiter=delimiter)
            if reader.fieldnames is None:
                raise ValueError("CSV sin cabecera (fieldnames es None)")
            if filter_column is not None and filter_column not in reader.fieldnames:
                raise KeyError(f"La columna de filtro '{filter_column}' no existe en el CSV")
            for row in reader:
                if filter_column is None or row.get(filter_column) == filter_value:
                    yield row
        finally:
            f.close()

    def _iter_keyed(kname: str) -> Iterator[Tuple[str, Dict[str, str]]]:
        f = open(path, encoding=encoding, newline='')
        try:
            reader = csv.DictReader(f, delimiter=delimiter)
            if reader.fieldnames is None:
                raise ValueError("CSV sin cabecera (fieldnames es None)")
            if kname not in reader.fieldnames:
                raise KeyError(f"La columna '{kname}' no existe en el CSV")
            if filter_column is not None and filter_column not in reader.fieldnames:
                raise KeyError(f"La columna de filtro '{filter_column}' no existe en el CSV")
            for row in reader:
                if filter_column is None or row.get(filter_column) == filter_value:
                    yield (row[kname], row)
        finally:
            f.close()

    # Validaciones rápidas de existencia/permisos
    if not os.path.exists(path):
        raise FileNotFoundError(f"Archivo no encontrado: {path}")

    try:
        # Validación de parámetros de filtrado
        if (filter_column is None) ^ (filter_value is None):
            raise ValueError("`filter_column` y `filter_value` deben proporcionarse juntos")

        if stream:
            if key is None:
                return _iter_rows()
            else:
                return _iter_keyed(key)

        # No streaming: cargar en memoria
        with open(path, encoding=encoding, newline='') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            if reader.fieldnames is None:
                raise ValueError("CSV sin cabecera (fieldnames es None)")

            if key is None:
                # Construir lista aplicando filtro si procede
                if filter_column is None:
                    return list(reader)
                return [r for r in reader if r.get(filter_column) == filter_value]

            if key not in reader.fieldnames:
                raise KeyError(f"La columna '{key}' no existe en el CSV")

            result: Dict[str, Any] = {}
            for row in reader:
                # Saltar filas que no cumplen el filtro (si se indicó)
                if filter_column is not None and row.get(filter_column) != filter_value:
                    continue

                k = row[key]
                if allow_duplicates:
                    result.setdefault(k, []).append(row)
                else:
                    if k in result:
                        raise ValueError(f"Valor duplicado '{k}' en la columna '{key}'")
                    result[k] = row

            return result

    except (csv.Error,) as e:
        raise csv.Error(f"Error parseando CSV '{path}': {e}")
    except PermissionError:
        raise

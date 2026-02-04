"""
Módulo `data_loader` — función `read_data`

Este módulo proporciona `read_data(filepath, fmt=None, index_col=None, required_fields=None)`
que soporta CSV y JSON y documenta cada paso del proceso.

Pasos documentados dentro de la función:
1) Determinar formato (extensión, parámetro `fmt`, o inspección del contenido).
2) Parsear el archivo según el formato detectado.
3) Para CSV: soportar columna índice opcional (`index_col`) y devolver un diccionario o lista.
4) Para JSON: validar campos opcionales (`required_fields`).
5) Manejar errores comunes con mensajes claros.

Ejemplo de uso:
>>> from laboratorio_4.data_loader import read_data
>>> read_data('datos.json')

El módulo está escrito en español y anotado paso a paso.
"""

from __future__ import annotations

import os
import json
import csv
import logging
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple, Union
import xml.etree.ElementTree as ET

_logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _detect_format(filepath: str, fmt: Optional[str] = None) -> str:
    """Detecta el formato del archivo: 'csv', 'json' o 'xml'.

    Intenta usar `fmt` si se proporciona, luego la extensión, y finalmente
    inspección del comienzo del archivo.
    """
    if fmt:
        fmt_lower = fmt.lower()
        if fmt_lower not in ("csv", "json", "xml"):
            raise ValueError(f"Formato desconocido: {fmt}")
        return fmt_lower

    lower = filepath.lower()
    if lower.endswith(".json"):
        return "json"
    if lower.endswith(".csv"):
        return "csv"
    if lower.endswith(".xml"):
        return "xml"

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            head = f.read(2048).lstrip()
            if not head:
                raise ValueError("Archivo vacío o no legible")
            if head[0] in ("{", "["):
                return "json"
            if head.startswith("<?xml") or head.startswith("<"):
                return "xml"
            return "csv"
    except FileNotFoundError:
        raise


def _read_json_full(filepath: str) -> Any:
    """Carga JSON completo en memoria (tolerante a errores)."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Error parsing JSON: {e}")


def _read_json_ndjson(filepath: str) -> Generator[Any, None, None]:
    """Generador para archivos NDJSON (una entidad JSON por línea)."""
    with open(filepath, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"JSON inválido en línea {i}: {e}")


def _read_json_stream_with_ijson(filepath: str) -> Generator[Any, None, None]:
    """Usa ijson si está disponible para parsear arrays JSON muy grandes.

    Devuelve un generador de elementos (items) de un array JSON superior.
    """
    try:
        import ijson  # type: ignore
    except Exception:  # pragma: no cover - optional dependency
        raise RuntimeError("Instale 'ijson' para parseo JSON en streaming (pip install ijson)")

    with open(filepath, "rb") as f:
        for item in ijson.items(f, "item"):
            yield item


def _read_csv_rows(filepath: str) -> Generator[Dict[str, Any], None, None]:
    """Generador de filas de CSV como diccionarios (usa iteración para ahorrar memoria)."""
    try:
        with open(filepath, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            if reader.fieldnames is None:
                raise ValueError("CSV sin encabezados detectables")
            for row in reader:
                yield row
    except csv.Error as e:
        raise ValueError(f"Error leyendo CSV: {e}")


def _read_xml_stream(filepath: str, item_tag: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
    """Parsea XML en streaming con `iterparse`.

    - Si `item_tag` se proporciona, yield para cada elemento con ese tag.
    - Si no, asume que los hijos directos del root son los items.
    Se devuelve un diccionario mapeando sub-etiquetas a texto/atributos.
    """
    try:
        context = ET.iterparse(filepath, events=("end",))
    except ET.ParseError as e:
        raise ValueError(f"Error parsing XML: {e}")

    for event, elem in context:
        if item_tag is None:
            # obtener root tag y procesar hijos directos: asumimos que los items son hijos del root
            parent = elem.getparent() if hasattr(elem, "getparent") else None
        # decidimos si este elemento debe ser yield
        tag = elem.tag
        if item_tag is None:
            # Si el element tiene elementos hijos, y su padre es root, lo tratamos como item
            if list(elem):
                item = {child.tag: (child.text.strip() if child.text else None) for child in elem}
                item.update({f"@{k}": v for k, v in elem.attrib.items()})
                yield item
                elem.clear()
        else:
            if tag == item_tag:
                item = {child.tag: (child.text.strip() if child.text else None) for child in elem}
                item.update({f"@{k}": v for k, v in elem.attrib.items()})
                yield item
                elem.clear()


def read_data(
    filepath: str,
    fmt: Optional[str] = None,
    index_col: Optional[Union[int, str]] = 0,
    required_fields: Optional[List[str]] = None,
    *,
    stream: bool = False,
    memory_threshold: int = 10_000_000,
    xml_item_tag: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Lee un archivo `CSV`, `JSON` o `XML` y devuelve un diccionario con la estructura:
      { 'format': 'csv'|'json'|'xml', 'data': <lista|dict|objeto_json|generator> }

    Características añadidas:
    - Soporte para `xml` (streaming con `iterparse`).
    - Streaming para archivos grandes: si `stream=True` o el tamaño del archivo
      supera `memory_threshold`, la función intentará devolver un generador en vez de
      cargar todo en memoria.
    - Para JSON, soporta NDJSON (newline-delimited JSON) y streaming por `ijson` si está instalado.
    - Manejo de errores con mensajes claros y logging.

    Parámetros importantes:
    - `stream`: forzar modo streaming. En streaming, `data` será un generador.
    - `memory_threshold`: tamaño en bytes a partir del cual se considera "grande".
    - `xml_item_tag`: tag de elementos XML a iterar; si no se suministra, se asume
      que los hijos directos del root son los items.

    Compatibilidad con parámetros anteriores: `index_col` sigue funcionando para CSV.
    """

    if not os.path.exists(filepath):
        raise FileNotFoundError(filepath)

    fmt_candidate = _detect_format(filepath, fmt)
    filesize = os.path.getsize(filepath)
    should_stream = stream or (filesize >= memory_threshold)

    _logger.debug("read_data: %s (fmt=%s, size=%d, stream=%s)", filepath, fmt_candidate, filesize, should_stream)

    # JSON handling
    if fmt_candidate == "json":
        # Detect NDJSON by peeking at first non-empty line
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped:
                    continue
                first_line = stripped
                break
            else:
                raise ValueError("Archivo JSON vacío")

        # NDJSON (one JSON object per line)
        if first_line.startswith("{") or first_line.startswith("[") and "}\n" not in first_line:
            # Could be NDJSON or regular JSON; prefer NDJSON if streaming
            if should_stream:
                # Try NDJSON streaming
                data_gen = _read_json_ndjson(filepath)
                return {"format": "json", "data": data_gen}
            # otherwise fallthrough to full load

        # If file is large and user asked streaming, try ijson
        if should_stream:
            try:
                data_gen = _read_json_stream_with_ijson(filepath)
                return {"format": "json", "data": data_gen}
            except RuntimeError as e:
                _logger.warning("ijson no disponible: %s; cargando en memoria como fallback", e)

        # Fallback: carga completa en memoria
        data = _read_json_full(filepath)
        # Validación opcional
        if required_fields:
            if isinstance(data, dict):
                missing = [f for f in required_fields if f not in data]
                if missing:
                    raise ValueError(f"Campos faltantes en JSON (objeto): {missing}")
            elif isinstance(data, list):
                for i, item in enumerate(data):
                    if not isinstance(item, dict):
                        raise ValueError(f"Elemento en JSON en posición {i} no es un objeto")
                    missing = [f for f in required_fields if f not in item]
                    if missing:
                        raise ValueError(f"Campos faltantes en JSON (fila {i}): {missing}")

        return {"format": "json", "data": data}

    # CSV handling
    if fmt_candidate == "csv":
        if should_stream:
            rows_gen = _read_csv_rows(filepath)
            if index_col is None:
                return {"format": "csv", "data": rows_gen}

            # need to wrap generator to produce keyed pairs
            def keyed_gen() -> Generator[Tuple[str, Dict[str, Any]], None, None]:
                # peek fieldnames by creating a reader quickly
                with open(filepath, newline="", encoding="utf-8") as csvfile:
                    reader = csv.DictReader(csvfile)
                    if reader.fieldnames is None:
                        raise ValueError("CSV sin encabezados detectables")
                    key_name = None
                    if isinstance(index_col, int):
                        if index_col < 0 or index_col >= len(reader.fieldnames):
                            raise ValueError("index_col fuera de rango")
                        key_name = reader.fieldnames[index_col]
                    else:
                        key_name = index_col
                        if key_name not in reader.fieldnames:
                            raise ValueError(f"index_col nombre no encontrado en encabezados: {key_name}")

                for row in _read_csv_rows(filepath):
                    if key_name not in row:
                        raise ValueError(f"Fila sin columna índice '{key_name}'")
                    key = row[key_name]
                    if key == "":
                        raise ValueError("Clave índice vacía en CSV")
                    row_copy = {k: v for k, v in row.items() if k != key_name}
                    yield key, row_copy

            return {"format": "csv", "data": keyed_gen()}

        # Non-streaming: load all
        try:
            with open(filepath, newline="", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                if reader.fieldnames is None:
                    raise ValueError("CSV sin encabezados detectables")
                rows = list(reader)
        except csv.Error as e:
            raise ValueError(f"Error leyendo CSV: {e}")

        if index_col is None:
            return {"format": "csv", "data": rows}

        if isinstance(index_col, int):
            if index_col < 0 or index_col >= len(reader.fieldnames):
                raise ValueError("index_col fuera de rango")
            key_name = reader.fieldnames[index_col]
        else:
            key_name = index_col
            if key_name not in reader.fieldnames:
                raise ValueError(f"index_col nombre no encontrado en encabezados: {key_name}")

        result: Dict[str, Dict[str, Any]] = {}
        for i, row in enumerate(rows):
            if key_name not in row:
                raise ValueError(f"Fila {i} sin columna índice '{key_name}'")
            key = row[key_name]
            if key == "":
                raise ValueError(f"Fila {i} tiene clave índice vacía")
            row_copy = {k: v for k, v in row.items() if k != key_name}
            result[key] = row_copy

        return {"format": "csv", "data": result}

    # XML handling
    if fmt_candidate == "xml":
        if should_stream:
            gen = _read_xml_stream(filepath, xml_item_tag)
            return {"format": "xml", "data": gen}
        # Non-streaming: parse whole tree
        try:
            tree = ET.parse(filepath)
            root = tree.getroot()
        except ET.ParseError as e:
            raise ValueError(f"Error parsing XML: {e}")

        # If xml_item_tag specified, collect those elements; otherwise, children of root
        items = []
        if xml_item_tag:
            for elem in root.findall(f".//{xml_item_tag}"):
                item = {child.tag: (child.text.strip() if child.text else None) for child in elem}
                item.update({f"@{k}": v for k, v in elem.attrib.items()})
                items.append(item)
        else:
            for child in list(root):
                item = {c.tag: (c.text.strip() if c.text else None) for c in child}
                item.update({f"@{k}": v for k, v in child.attrib.items()})
                items.append(item)

        return {"format": "xml", "data": items}

    raise ValueError(f"Formato no soportado: {fmt_candidate}")

# Laboratorio 4 — Data Loader

Este pequeño README explica cómo usar `data_loader.read_data` para leer archivos CSV y JSON.

Ejemplos rápidos:

- Leer JSON (archivo que contiene un objeto o una lista):

```python
from laboratorio_4.data_loader import read_data

res = read_data('ruta/a/archivo.json')
print(res['format'])  # 'json'
print(res['data'])
```

- Leer CSV y devolver una lista de filas (sin índice):

```python
from laboratorio_4.data_loader import read_data

res = read_data('ruta/a/archivo.csv', index_col=None)
print(res['format'])  # 'csv'
print(type(res['data']))  # list
```

- Leer CSV usando la primera columna como índice (comportamiento por defecto):

```python
from laboratorio_4.data_loader import read_data

res = read_data('ruta/a/archivo_con_indice.csv')
print(res['format'])  # 'csv'
print(type(res['data']))  # dict
for key, row in res['data'].items():
    print(key, row)
```

Validación de campos para JSON:

```python
res = read_data('datos.json', required_fields=['id','name'])
# Si faltan campos, la función lanza ValueError con información útil
```

Consejos:
- Si tu archivo no tiene extensión, la función intentará inferirlo leyendo el comienzo del archivo.
- En CSV la función requiere encabezados (column names) para trabajar con `DictReader`.

Soporte adicional:

- XML: ahora `read_data` soporta archivos XML. Por defecto carga los hijos del `root` como items.
    Para iterar elementos concretos use `xml_item_tag` o `stream=True` para procesar en streaming.

- Streaming / archivos grandes:
    - Parámetro `stream=True` fuerza que `data` sea un generador en vez de cargar todo en memoria.
    - Si el archivo es mayor que `memory_threshold` (por defecto 10_000_000 bytes), la función
        intentará devolver un generador automáticamente.
    - Para JSON en streaming se usa NDJSON o `ijson` si está instalado (recomendado para arrays enormes).

Ejemplo rápido de streaming CSV:

```python
res = read_data('ruta/a/archivo.csv', stream=True, index_col=None)
# res['data'] es un generador: consúmelo con un bucle for
for row in res['data']:
        print(row)
```

Ejemplo rápido de streaming XML:

```python
res = read_data('ruta/a/archivo.xml', stream=True, xml_item_tag='person')
for item in res['data']:
        print(item)
```

Si quieres, puedo añadir ejemplos de test o un pequeño script de demostración. ¿Lo añado? 

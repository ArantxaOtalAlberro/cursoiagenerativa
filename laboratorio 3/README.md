Proyecto: Laboratorio 3

Contenido:
- `utils.py`: contiene `read_csv_to_dict` mejorada.
- `main.py`: demo de uso (lee `sample.csv`).
- `sample.csv`: CSV de ejemplo.

Cómo ejecutar:

```bash
python "laboratorio 3/main.py"
```

Nota: Ejecuta el comando desde la raíz del workspace (`curso-ia-generativa`).

Ejemplos de uso de filtrado:

1) Leer sólo filas con `name == 'Ana'` (devuelve lista):

```python
from utils import read_csv_to_dict
rows = read_csv_to_dict('sample.csv', filter_column='name', filter_value='Ana')
print(rows)
```

2) Leer y obtener un diccionario indexado por `id`, filtrando por edad:

```python
from utils import read_csv_to_dict
by_id = read_csv_to_dict('sample.csv', key='id', filter_column='age', filter_value='25')
print(by_id)
```

3) Streaming (no carga todo en memoria) y filtrado:

```python
it = read_csv_to_dict('sample.csv', stream=True, filter_column='name', filter_value='Juan')
for row in it:
	print(row)
```

Ejecutar sin interacción usando el script auxiliar:

```bash
./laboratorio\ 3/run_lab3.sh Alice 5
```

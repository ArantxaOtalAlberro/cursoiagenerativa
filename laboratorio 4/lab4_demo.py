#!/usr/bin/env python3
import os
import sys

# Aseguramos que el directorio actual del script esté en sys.path
HERE = os.path.dirname(__file__)
sys.path.insert(0, HERE)


from data_loader import read_data


def drain_and_print(gen, limit: int = 5):
    """Helper: consumir el generador hasta `limit` elementos para mostrar en demo."""
    out = []
    for i, item in enumerate(gen):
        if i >= limit:
            out.append("...")
            break
        out.append(item)
    return out


def main():
    print("Demo: leyendo sample.json")
    res = read_data(os.path.join(HERE, "sample.json"))
    print("format:", res["format"])
    print("data:", res["data"])

    print("\nDemo: leyendo sample.csv con índice (por defecto)")
    res2 = read_data(os.path.join(HERE, "sample.csv"))
    print("format:", res2["format"])
    print("data:", res2["data"])

    print("\nDemo: leyendo sample.csv en streaming (generador de filas)")
    res4 = read_data(os.path.join(HERE, "sample.csv"), stream=True, index_col=None)
    print("format:", res4["format"])
    print("data (primeros elementos):", drain_and_print(res4["data"]))

    print("\nDemo: leyendo sample.xml (no-streaming)")
    res_xml = read_data(os.path.join(HERE, "sample.xml"))
    print("format:", res_xml["format"])
    print("data:", res_xml["data"])

    print("\nDemo: leyendo sample.xml en streaming (xml_item_tag='person')")
    res_xml_stream = read_data(os.path.join(HERE, "sample.xml"), stream=True, xml_item_tag="person")
    print("format:", res_xml_stream["format"])
    print("data (primeros elementos):", drain_and_print(res_xml_stream["data"]))


if __name__ == "__main__":
    main()

import os
import sys
import pytest

HERE = os.path.dirname(__file__)
# allow importing `utils.py` in the same folder
sys.path.insert(0, HERE)

from utils import read_csv_to_dict  # noqa: E402


def test_read_list():
    res = read_csv_to_dict(os.path.join(HERE, "sample.csv"))
    assert isinstance(res, list)
    assert len(res) == 2
    assert any(r["name"] == "Ana" for r in res)


def test_read_keyed():
    res = read_csv_to_dict(os.path.join(HERE, "sample.csv"), key="id")
    assert isinstance(res, dict)
    assert res["1"]["name"] == "Ana"


def test_filtering():
    res = read_csv_to_dict(
        os.path.join(HERE, "sample.csv"), filter_column="name", filter_value="Ana"
    )
    assert isinstance(res, list)
    assert len(res) == 1
    assert res[0]["name"] == "Ana"


def test_stream_filtered():
    it = read_csv_to_dict(
        os.path.join(HERE, "sample.csv"),
        stream=True,
        filter_column="name",
        filter_value="Juan",
    )
    first = next(it)
    assert first["name"] == "Juan"


def test_invalid_filter_params():
    with pytest.raises(ValueError):
        read_csv_to_dict(os.path.join(HERE, "sample.csv"), filter_column="name")


def test_stream_missing_filter_column_raises():
    with pytest.raises(KeyError):
        # streaming path validates filter_column existence against headers
        list(
            read_csv_to_dict(
                os.path.join(HERE, "sample.csv"),
                stream=True,
                filter_column="nonexistent",
                filter_value="x",
            )
        )


def test_allow_duplicates(tmp_path):
    p = tmp_path / "dup.csv"
    p.write_text("id,name\n1,A\n1,B\n")
    res = read_csv_to_dict(str(p), key="id", allow_duplicates=True)
    assert isinstance(res["1"], list)
    assert len(res["1"]) == 2

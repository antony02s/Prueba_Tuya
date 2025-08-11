import pandas as pd
from src.extract.read_files import read_csv

def test_read_csv(tmp_path):
    # Crear CSV temporal
    csv_file = tmp_path / "sample.csv"
    pd.DataFrame({"id": [1, 2], "name": ["A", "B"]}).to_csv(csv_file, index=False)

    df = read_csv(csv_file)
    assert not df.empty
    assert list(df.columns) == ["id", "name"]
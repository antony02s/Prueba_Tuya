import pandas as pd
import pytest
from src.validate.checks import assert_not_null, ValidationError

def test_assert_not_null_pass():
    df = pd.DataFrame({"id": [1, 2], "status": ["ok", "ok"]})
    assert_not_null(df, ["id"])

def test_assert_not_null_fail():
    df = pd.DataFrame({"id": [1, None]})
    with pytest.raises(ValidationError):
        assert_not_null(df, ["id"])
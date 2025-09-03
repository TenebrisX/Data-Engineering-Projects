from src.main import _ms_to_date_id

def test_ms_to_date_id():
    assert _ms_to_date_id(1704067200000) == 20240101  # 2024-01-01 UTC

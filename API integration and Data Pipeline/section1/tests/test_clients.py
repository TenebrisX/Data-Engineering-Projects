from src.clients.frankfurter_client import FrankfurterClient

def test_frankfurter_time_series_smoke():
    fx = FrankfurterClient()
    js = fx.time_series("2024-01-01", "2024-01-05", base="EUR", symbols=["USD"])
    assert "rates" in js
    assert js["base"] == "EUR"

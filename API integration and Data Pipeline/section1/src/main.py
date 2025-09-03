from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

from src.clients.polygon_client import PolygonClient
from src.clients.frankfurter_client import FrankfurterClient
from src.loaders.duckdb_loader import DuckDBLoader
from src.config import settings
from src.utils.logging import init_logger

logger = init_logger(__name__)

def _ms_to_date_id(ms: int) -> int:
    """Convert a timestamp in milliseconds to an integer date ID (YYYYMMDD).

    Args:
        ms (int): Timestamp in milliseconds since epoch.

    Returns:
        int: Date ID in YYYYMMDD format.
    """
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()
    return int(dt.strftime("%Y%m%d"))

def extract_polygon_to_parquet(ticker: str, start: str, end: str) -> Path:
    """Extract OHLCV data from Polygon and save it as a Parquet file.

    Args:
        ticker (str): Ticker symbol (e.g., "AAPL").
        start (str): Start date in YYYY-MM-DD format.
        end (str): End date in YYYY-MM-DD format.

    Returns:
        Path: Path to the generated Parquet file.

    Raises:
        RuntimeError: If no data is returned from Polygon API.
    """
    client = PolygonClient()
    rows = []
    for r in client.list_aggs(ticker, start, end, timespan=settings.polygon_timespan):
        rows.append({
            "ticker": ticker.upper(),
            "ts_ms": r["t"], "open": r.get("o"), "high": r.get("h"),
            "low": r.get("l"), "close": r.get("c"), "volume": r.get("v"),
            "vwap": r.get("vw"), "source": "polygon"
        })
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No polygon data fetched")
    df["date_id"] = df["ts_ms"].apply(_ms_to_date_id)
    out = settings.out_dir / f"stg_stock_prices_{ticker}_{start}_{end}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.drop(columns=["ts_ms"]).to_parquet(out, index=False)
    logger.info("Wrote %s rows to %s", len(df), out)
    return out

def extract_fx_to_parquet(start: str, end: str, base: str = "EUR", symbols: list[str] | None = None) -> Path:
    """Fetch historical FX rates from Frankfurter API and save as Parquet.

    Args:
        start (str): Start date in YYYY-MM-DD format.
        end (str): End date in YYYY-MM-DD format.
        base (str, optional): Base currency. Defaults to "EUR".
        symbols (list[str] | None, optional): List of quote currencies. If None, fetches all available.

    Returns:
        Path: Path to the generated FX rates Parquet file.
    """
    fx = FrankfurterClient()
    js = fx.time_series(start, end, base=base, symbols=symbols)
    rates = []
    for d, m in js["rates"].items():
        for quote, rate in m.items():
            rates.append({"date_id": int(d.replace("-", "")), "base_currency": js["base"], "quote_currency": quote, "rate": float(rate)})
    df = pd.DataFrame(rates)
    out = settings.out_dir / f"stg_fx_rates_{start}_{end}_{base}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    logger.info("Wrote %s rows to %s", len(df), out)
    return out

def load_dw(stock_stage: Path, fx_stage: Path, dw_path: Path, schema_sql: Path) -> None:
    """
    Load staged stock price and FX rate data into a DuckDB-based data warehouse.

    This function performs the full ETL process from Parquet staging files into the DuckDB data warehouse,
    including:
    - Initializing the DW schema from a provided SQL file.
    - Loading and materializing staging views for stock and FX data.
    - Inserting new records into dimension tables (`dim_currency`, `dim_ticker`).
    - Performing an idempotent upsert (delete + insert) into the fact table `fact_stock_price_daily`.
    - Performing an upsert into the FX rate fact table `fx_rate_daily` using `ON CONFLICT`.
    - Logging table health and performing basic data quality checks (e.g., invalid OHLC values, missing FX).

    Args:
        stock_stage (Path): Path to the staged stock price Parquet file.
        fx_stage (Path): Path to the staged FX rates Parquet file.
        dw_path (Path): Path to the DuckDB database file.
        schema_sql (Path): Path to the SQL schema definition script.

    Raises:
        RuntimeError: If any critical data quality checks fail (e.g., OHLC integrity or FX join issues).
    """
    loader = DuckDBLoader(dw_path)
    loader.init_schema(schema_sql)

    con = loader.connect()
    try:
        loader.load_stage_parquet(con, "stg_prices", stock_stage)
        loader.load_stage_parquet(con, "stg_fx", fx_stage)
        # dims
        logger.info("loading_dim_currency")
        con.execute("""
            insert or ignore into dim_currency(currency_code, currency_name)
            select distinct quote_currency, quote_currency from stg_fx
            union
            select 'EUR','Euro';
        """)

        logger.info("loading_dim_ticker")
        con.execute("""
            insert or ignore into dim_ticker(ticker) select distinct ticker from stg_prices;
        """)

        # fact upsert (delete/insert demo)
        logger.info("upserting_fact_stock_price_daily")
        con.execute("""
            delete from fact_stock_price_daily
            using stg_prices s
            where fact_stock_price_daily.ticker = s.ticker
              and fact_stock_price_daily.date_id = s.date_id;
        """)
        con.execute("""
            insert into fact_stock_price_daily
            select
              date_id, ticker, 'USD' as currency_code,
              open, high, low, close, vwap, volume, 'polygon' as source, current_timestamp
            from stg_prices;
        """)

        logger.info("upserting_fx_rate_daily")
        con.execute("""
            insert into fx_rate_daily
            select date_id, base_currency, quote_currency, rate, current_timestamp
            from stg_fx
            on conflict (date_id, base_currency, quote_currency) do update set
              rate=excluded.rate, load_ts=current_timestamp;
        """)

        # log post-load health NOTE: should move to tests
        loader.log_table_health(con, [
            "dim_currency", "dim_ticker", "fact_stock_price_daily", "fx_rate_daily"
        ])

        bad_ohlc = con.execute("""
            select count(*) from fact_stock_price_daily
            where high < low or open < low or open > high or close < low or close > high
        """).fetchone()[0]
        if bad_ohlc:
            logger.error("dq_ohlc_failed", extra={"bad_rows": int(bad_ohlc)})

        missing_fx = con.execute("""
            select count(*) from fact_stock_price_daily f
            left join fx_rate_daily fx_native
              on fx_native.date_id=f.date_id and fx_native.base_currency='EUR' and fx_native.quote_currency=f.currency_code
            where fx_native.rate is null
        """).fetchone()[0]
        if missing_fx:
            logger.error("dq_missing_fx", extra={"rows_missing_fx": int(missing_fx)})

        logger.info("load_dw_complete")

    finally:
        con.close()

def run(end: str, days_back: int, ticker: str = "AAPL", base: str = "EUR") -> None:
    """Execute the end-to-end ETL pipeline from API extraction to DW load.

    Args:
        end (str): End date (inclusive) for the ETL window in YYYY-MM-DD format.
        days_back (int): Number of days of history to fetch including `end`.
        ticker (str, optional): Stock ticker to extract. Defaults to "AAPL".
        base (str, optional): Base currency for FX rates. Defaults to "EUR".
    """
    start = pd.to_datetime(end) - pd.Timedelta(days=days_back - 1)
    start_s = start.strftime("%Y-%m-%d")
    stock_stage = extract_polygon_to_parquet(ticker, start_s, end)
    fx_stage = extract_fx_to_parquet(start_s, end, base=base, symbols=None)
    load_dw(stock_stage, fx_stage, settings.duckdb_path, Path("sql/dw_schema_duckdb.sql"))

if __name__ == "__main__":
    run(end=pd.Timestamp.utcnow().date().strftime("%Y-%m-%d"), days_back=30, ticker="AAPL", base="EUR")

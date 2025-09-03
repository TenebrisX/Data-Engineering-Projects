/*
    SQL Data Warehouse Schema for Section 1 API Integration and Data Pipeline

    This script defines the dimensional model and supporting views
    to store and analyze daily stock prices and foreign exchange rates.

    Schema Overview:
    - dim_date: Calendar dimension from 1990-01-01 to 2050-12-31
    - dim_currency: List of currencies and their names
    - dim_ticker: List of stock tickers with metadata
    - fact_stock_price_daily: Daily price and volume information for tickers
    - fx_rate_daily: Exchange rates between currency pairs by day
    - v_stock_price_in_currency: View to compute price in any target currency

    Tables are designed for analytical workloads, enabling queries such as:
    - Historical price comparisons in a target currency
    - FX rate adjustments for multi-currency instruments
    - Aggregated metrics by time and instrument

    Foreign keys ensure referential integrity across the schema.
    Default timestamps and indexing support ETL tracking and performance.
*/

-- Drops
drop table if exists stock_data.main.fx_rate_daily cascade;
drop table if exists stock_data.main.fact_stock_price_daily cascade;
drop table if exists stock_data.main.dim_date cascade;
drop table if exists stock_data.main.dim_ticker cascade;
drop table if exists stock_data.main.dim_currency cascade;



-- dim_date: Date dimension with surrogate key and calendar attributes
create table if not exists stock_data.main.dim_date as
select
  cast(strftime('%Y%m%d', d) as int) as date_id,
  d as full_date,
  cast(strftime('%Y', d) as int) as year,
  cast(((cast(strftime('%m', d) as int)-1)/3)+1 as int) as quarter,
  cast(strftime('%m', d) as int) as month,
  cast(strftime('%d', d) as int) as day,
  case when strftime('%w', d) in ('0','6') then 1 else 0 end as is_weekend
from generate_series(date '1990-01-01', date '2050-12-31', interval 1 day) as t(d);

alter table stock_data.main.dim_date add constraint pk_dim_date primary key (date_id);

-- dim_currency: Currency dimension with code and name
create table if not exists stock_data.main.dim_currency(
  currency_code text primary key,
  currency_name text
);

-- dim_ticker: Ticker dimension (stock symbol, exchange, and name)
create table if not exists stock_data.main.dim_ticker(
  ticker text primary key,
  exchange text,
  name text
);

-- fact_stock_price_daily: Main fact table with stock OHLCV data
create table if not exists stock_data.main.fact_stock_price_daily(
  date_id int references dim_date(date_id),
  ticker text references dim_ticker(ticker),
  currency_code text references dim_currency(currency_code),
  open double,
  high double,
  low double,
  close double,
  vwap double,
  volume double,
  source text not null default 'unknown',
  load_ts timestamp not null default get_current_timestamp(),

  constraint ck_nonneg_volume check (volume is null or volume >= 0),
  constraint ck_nonneg_prices check (
    (open is null or open >= 0) and
    (high is null or high >= 0) and
    (low is null or low >= 0) and
    (close is null or close >= 0) and
    (vwap is null or vwap >= 0)
  ),
  constraint ck_ohlc_ordering check (
    (open is null or high is null or low is null) 
    or (low <= open and open <= high)
  ),
    constraint ck_close_in_range check (
    (close is null or high is null or low is null)
    or (low <= close and close <= high)
  )
);

-- Index to support efficient queries by ticker and date
create index if not exists idx_fact_by_ticker_date on fact_stock_price_daily(ticker, date_id);

-- fx_rate_daily: FX rates per currency pair per day
create table if not exists stock_data.main.fx_rate_daily(
  date_id int references dim_date(date_id),
  base_currency text references dim_currency(currency_code),
  quote_currency text references dim_currency(currency_code),
  rate double,
  load_ts timestamp default get_current_timestamp(),
  primary key(date_id, base_currency, quote_currency),

  constraint ck_fx_positive check (rate > 0.0),
  constraint ck_fx_base_quote_diff check (lower(base_currency) <> lower(quote_currency))
);

-- View: v_stock_price_in_currency
-- Dynamically computes the stock closing price in any target currency
-- Formula: price_in(target_ccy) = native_price * rate(EUR→target) / rate(EUR→native)
create or replace view stock_data.main.v_stock_price_in_currency as
select
  f.date_id,
  d.full_date,
  f.ticker,
  f.close as close_native,
  f.currency_code as native_ccy,
  target.quote_currency as target_ccy,
  f.close * target.rate / native.rate as close_converted
from stock_data.main.fact_stock_price_daily f
join stock_data.main.fx_rate_daily native
  on native.date_id = f.date_id and native.base_currency = 'EUR' and native.quote_currency = f.currency_code
join fx_rate_daily target
  on target.date_id = f.date_id and target.base_currency = native.base_currency
join dim_date d
  on d.date_id = f.date_id;

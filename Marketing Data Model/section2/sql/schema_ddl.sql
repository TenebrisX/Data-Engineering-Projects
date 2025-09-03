-- ============================================================================
-- Schema: marketdb.main
-- Purpose: Dimensional model for ad campaign performance across platforms.
--           This schema supports tracking ad hierarchies, performance metrics,
--           control parameters, and temporal dimensions for reporting & analysis.
--
-- Overview:
--   This schema implements a classic star schema with:
--     - Slowly changing dimensions (SCD2) for entities like accounts, campaigns, ads.
--     - Snapshot hierarchy bridge table (`bridge_ad_hierarchy`) for point-in-time FK joins.
--     - Fact tables capturing ad performance and control settings at daily/hourly grains.
--
-- Dimensions:
--   - dim_platform: Logical platforms (e.g., Google, Twitter).
--   - dim_date, dim_hour: Temporal dimensions.
--   - dim_device, dim_geo: User environment dimensions.
--   - dim_account to dim_ad: Ad entity hierarchy (with SCD2 support).
--   - dim_label: Optional metadata labels for ads.
--
-- Bridge:
--   - bridge_ad_hierarchy: Denormalized FK chain for current ad hierarchy state.
--
-- Facts:
--   - fact_ad_performance_day: Metrics like impressions, clicks, cost at daily grain.
--   - fact_ad_controls_hour: Bid/control settings at hourly grain.
--
-- Notable Features:
--   - All SCD2 dimension tables include `effective_from`, `effective_to`, and `is_current`.
--   - Composite uniqueness enforced across platform IDs and native keys + effective dates.
--   - Data quality enforced via CHECK constraints (e.g., positive cost/clicks).
--   - Pre-built indexes support common query patterns (e.g., date/platform/ad).
--
-- Assumptions:
--   - Platform-native IDs are stable identifiers provided by each platform's API.
--   - Date/hour dimension tables are pre-populated and static.
--   - SCD2 tracking is handled upstream.
--
-- Usage Notes:
--   - Facts should always reference the `is_current = true` version of dimension entities.
--   - `bridge_ad_hierarchy` is refreshed daily to reflect the latest SCD2 joins.
--   - Use surrogate IDs for all joins; platform-native IDs are not stable enough.
-- ============================================================================

--drops
drop table if exists marketdb.main.fact_ad_controls_hour cascade;
drop table if exists marketdb.main.fact_ad_performance_day cascade;
drop table if exists marketdb.main.bridge_ad_hierarchy cascade;

drop table if exists marketdb.main.dim_label cascade;
drop table if exists marketdb.main.dim_ad cascade;
drop table if exists marketdb.main.dim_ad_group cascade;
drop table if exists marketdb.main.dim_campaign cascade;
drop table if exists marketdb.main.dim_portfolio cascade;
drop table if exists marketdb.main.dim_sub_account cascade;
drop table if exists marketdb.main.dim_account cascade;

drop table if exists marketdb.main.dim_geo cascade;
drop table if exists marketdb.main.dim_device cascade;
drop table if exists marketdb.main.dim_hour cascade;
drop table if exists marketdb.main.dim_date cascade;
drop table if exists marketdb.main.dim_platform cascade;

-- dims
create table marketdb.main.dim_platform (
  platform_id   bigint primary key,
  platform_code varchar(32)  not null unique,
  platform_name varchar(64)  not null
);

create table marketdb.main.dim_date (
  date_id     int primary key,
  date_actual date not null,
  year        int not null,
  quarter     int not null check (quarter between 1 and 4),
  month       int not null check (month between 1 and 12),
  day         int not null check (day between 1 and 31),
  iso_week    int not null,
  is_weekend  boolean not null default false
);

create table marketdb.main.dim_hour (
  hour_id     int primary key,
  date_id     int not null references dim_date(date_id),
  hour_of_day int not null check (hour_of_day between 0 and 23)
);

create table marketdb.main.dim_device (
  device_id   bigint primary key,
  device_code varchar(32)  not null unique,
  device_name varchar(64)  not null
);

create table marketdb.main.dim_geo (
  geo_id   bigint primary key,
  geo_code varchar(32)  not null unique,
  geo_name varchar(128) not null
  -- Extend with region/country if needed
);

-- SCD2: Natural key = platform_id + platform_native_id + effective_from
create table marketdb.main.dim_account (
  account_id            bigint primary key,
  platform_id           bigint not null references dim_platform(platform_id),
  platform_account_id   varchar(128) not null,
  account_name          varchar(256) not null,
  effective_from        timestamp not null,
  effective_to          timestamp not null,
  is_current            boolean not null,
  unique (platform_id, platform_account_id, effective_from)
);

create table marketdb.main.dim_sub_account (
  sub_account_id          bigint primary key,
  account_id              bigint not null references dim_account(account_id),
  platform_id             bigint not null references dim_platform(platform_id),
  platform_sub_account_id varchar(128) not null,
  sub_account_name        varchar(256) not null,
  effective_from          timestamp not null,
  effective_to            timestamp not null,
  is_current              boolean not null,
  unique (platform_id, platform_sub_account_id, effective_from)
);

create table marketdb.main.dim_portfolio (
  portfolio_id          bigint primary key,
  sub_account_id        bigint not null references dim_sub_account(sub_account_id),
  platform_id           bigint not null references dim_platform(platform_id),
  platform_portfolio_id varchar(128) not null,
  portfolio_name        varchar(256) not null,
  effective_from        timestamp not null,
  effective_to          timestamp not null,
  is_current            boolean not null,
  unique (platform_id, platform_portfolio_id, effective_from)
);

create table marketdb.main.dim_campaign (
  campaign_id           bigint primary key,
  portfolio_id          bigint not null references dim_portfolio(portfolio_id),
  platform_id           bigint not null references dim_platform(platform_id),
  platform_campaign_id  varchar(128) not null,
  campaign_name         varchar(256) not null,
  effective_from        timestamp not null,
  effective_to          timestamp not null,
  is_current            boolean not null,
  unique (platform_id, platform_campaign_id, effective_from)
);

create table marketdb.main.dim_ad_group (
  ad_group_id           bigint primary key,
  campaign_id           bigint not null references dim_campaign(campaign_id),
  platform_id           bigint not null references dim_platform(platform_id),
  platform_ad_group_id  varchar(128) not null,
  ad_group_name         varchar(256) not null,
  effective_from        timestamp not null,
  effective_to          timestamp not null,
  is_current            boolean not null,
  unique (platform_id, platform_ad_group_id, effective_from)
);

create table marketdb.main.dim_ad (
  ad_id          bigint primary key,
  ad_group_id    bigint not null references dim_ad_group(ad_group_id),
  platform_id    bigint not null references dim_platform(platform_id),
  platform_ad_id varchar(128) not null,
  ad_name        varchar(256) not null,
  effective_from timestamp not null,
  effective_to   timestamp not null,
  is_current     boolean not null,
  unique (platform_id, platform_ad_id, effective_from)
);

create table marketdb.main.dim_label (
  label_id   bigint primary key,
  label_text varchar(32) not null unique
);

-- Hierarchy snapshot: point-in-time view of FK chain
create table marketdb.main.bridge_ad_hierarchy (
  ad_id             bigint primary key references dim_ad(ad_id),
  ad_group_id       bigint not null references dim_ad_group(ad_group_id),
  campaign_id       bigint not null references dim_campaign(campaign_id),
  portfolio_id      bigint not null references dim_portfolio(portfolio_id),
  sub_account_id    bigint not null references dim_sub_account(sub_account_id),
  account_id        bigint not null references dim_account(account_id),
  platform_id       bigint not null references dim_platform(platform_id),
  last_refreshed_at timestamp not null default now()
);

-- facts
create table marketdb.main.fact_ad_performance_day (
  ad_perf_day_id  bigint primary key,
  platform_id     bigint not null references dim_platform(platform_id),
  ad_id           bigint not null references dim_ad(ad_id),
  stat_date_id    int    not null references dim_date(date_id),
  click_date_id   int    null references dim_date(date_id),
  view_date_id    int    null references dim_date(date_id),
  device_id       bigint not null references dim_device(device_id),
  geo_id          bigint not null references dim_geo(geo_id),
  impressions     bigint not null check (impressions >= 0),
  clicks          bigint not null check (clicks >= 0),
  cost            numeric(18,6) not null check (cost >= 0),
  src_hash        varchar(64) not null,
  load_ts         timestamp not null default now(),

  unique (platform_id, ad_id, stat_date_id, device_id, geo_id)
);

create table marketdb.main.fact_ad_controls_hour (
  ad_ctrl_hour_id bigint primary key,
  platform_id     bigint not null references dim_platform(platform_id),
  ad_id           bigint not null references dim_ad(ad_id),
  hour_id         int    not null references dim_hour(hour_id),
  bid             numeric(18,6) not null check (bid >= 0),
  label_id        bigint null references dim_label(label_id),
  src_hash        varchar(64) not null,
  load_ts         timestamp not null default now(),

  unique (platform_id, ad_id, hour_id)
);

-- indexes
create index if not exists idx_fact_ad_performance_day_a on fact_ad_performance_day (stat_date_id, platform_id, ad_id);
create index if not exists idx_fact_ad_performance_day_b on fact_ad_performance_day (platform_id, device_id, geo_id);
create index if not exists idx_fact_ad_controls_hour     on fact_ad_controls_hour (hour_id, platform_id, ad_id);

create index if not exists idx_dim_ad            on dim_ad (platform_id, platform_ad_id, is_current);
create index if not exists idx_dim_ad_group      on dim_ad_group (platform_id, platform_ad_group_id, is_current);
create index if not exists idx_dim_campaign      on dim_campaign (platform_id, platform_campaign_id, is_current);
create index if not exists idx_dim_portfolio     on dim_portfolio (platform_id, platform_portfolio_id, is_current);
create index if not exists idx_dim_sub_account   on dim_sub_account (platform_id, platform_sub_account_id, is_current);
create index if not exists idx_dim_account       on dim_account (platform_id, platform_account_id, is_current);

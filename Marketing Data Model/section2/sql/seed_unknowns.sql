insert into dim_date(date_id, date_actual, year, quarter, month, day, iso_week, is_weekend)
values (0, '1970-01-01', 1970, 1, 1, 1, 1, false) on conflict do nothing;

insert into dim_hour(hour_id, date_id, hour_of_day)
values (0, 0, 0) on conflict do nothing;

insert into dim_device(device_id, device_code, device_name)
values (0, 'unknown', 'Unknown') on conflict do nothing;

insert into dim_geo(geo_id, geo_code, geo_name)
values (0, 'ZZ', 'Unknown') on conflict do nothing;

insert into dim_platform(platform_id, platform_code, platform_name)
values (0, 'unknown', 'Unknown') on conflict do nothing;

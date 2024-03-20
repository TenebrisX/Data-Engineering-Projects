--Data mart ddl

create table if not exists dm_rfm_segments (
user_id int4 not null,
recency int4 not null,
frequency int4 not null,
monetary_value int4 not null,

constraint dm_rfm_segments_user_id_pk primary key(user_id),
constraint dm_rfm_segments_recency_check check(recency >= 1 and recency <= 5),
constraint dm_rfm_segments_frequency_check check(frequency >= 1 and frequency <= 5),
constraint dm_rfm_segments_monetary_value_check check(monetary_value >= 1 and monetary_value <= 5)
);


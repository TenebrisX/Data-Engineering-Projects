-- dm_rfm_segments table fill

--truncate table analysis.dm_rfm_segments;
insert into analysis.dm_rfm_segments
select trr.user_id, 
	   trr.recency,
	   trf.frequency,
	   trmv.monetary_value
from analysis.tmp_rfm_recency trr 
left join analysis.tmp_rfm_frequency trf 
	on trr.user_id = trf.user_id
left join analysis.tmp_rfm_monetary_value trmv
	on trmv.user_id = trr.user_id;


-- 10 first rows
select *
from analysis.dm_rfm_segments drs
order by user_id
limit 10;

ser_id|recency|frequency|monetary_value|
------+-------+---------+--------------+
     0|      1|        3|             4|
     1|      4|        3|             3|
     2|      2|        3|             5|
     3|      2|        3|             3|
     4|      4|        3|             3|
     5|      5|        5|             5|
     6|      1|        3|             5|
     7|      4|        2|             2|
     8|      1|        1|             3|
     9|      2|        2|             2|
select 
    count(case when customer_id is null then 1 end) nulls
from staging.user_activity_log;
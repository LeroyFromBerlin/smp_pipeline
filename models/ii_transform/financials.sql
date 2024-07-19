/* There are customer_id's that have different financials depending on device_id. Example: customer_id = 100.
Device_id could be a way of linking to different financials in different markets but it does not show in any of the other tables. 
There is no way for me to knowing which row to use in such cases so I'll use an average. */

select customer_id, avg(FIVE_YEAR_LTV) as FIVE_YEAR_LTV, avg(CAC) as CAC, count(*) as c
from {{ source('S1_INPUT', 'RAW_FINANCIALS') }}
group by 1;

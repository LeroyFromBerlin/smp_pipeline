/* There are customers that have been acquired multiple times (in line with the funnel table). 
I assume that each row represents a separate market. 
Since the data is incomplete with rows and columns missing I can't join this to the right funnel because of the missing country column. 
I'll only keep the first acquisition per customer_id to enable a 1-on-1 join in later steps and drop the later rows. 
There turns out to be 1 customer (cid 194) that has two entries with the very same acquisition_date. 
I'll leave it in for now but keep it on the radar to decide later if that poses a problem */

-- only keep earliest instances
select a.*
from {{ source('S1_INPUT', 'RAW_SALES') }} a
inner join (select customer_id, min(acquisition_date) as acquisition_date FROM {{ source('S1_INPUT', 'RAW_SALES') }} group by 1) b 
on a.customer_id = b.customer_id and a.acquisition_date = b.acquisition_date;

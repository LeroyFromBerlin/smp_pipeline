/* There is a bunch of customer_id's that have separate entries for separate countries and typologies. 
I'll assume that there should only one typology per customer_id in a given market and keep the latest one. 
I do keep all markets, though */

-- only keep latest instances
select a.*
from {{ source('S1_INPUT', 'RAW_STORE') }} a
inner join (select customer_id, country, max(created_at) as created_at FROM {{ source('S1_INPUT', 'RAW_STORE') }} group by 1, 2) b 
on a.customer_id = b.customer_id and a.country = b.country and a.created_at = b.created_at;



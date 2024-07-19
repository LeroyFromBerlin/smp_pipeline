select a.customer_id, a.typology, a.lead_creation_date, a.live_date, datediff('day', a.lead_creation_date, a.live_date) as daydelta, case when b.rank <= 5 then 1 else 0 end as is_top_5, b.rank
from {{ ref(funnel) }} a
left join (select typology, count_orders, row_number() over (order by count_orders desc) rank from (select typology, count(*) as count_orders from {{ ref(funnel) }} group by 1 order by 2 desc)) 
b on a.typology = b.typology;
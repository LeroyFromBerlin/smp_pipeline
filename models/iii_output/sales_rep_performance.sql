select a.sales_email, a.customer_id, b.customer_id as customer_id_b, b.five_year_ltv, b.cac, b.five_year_ltv - b.cac as ltv_after_cac
from {{ ref(funnel) }} a
left join {{ ref(financials) }} b on a.customer_id = b.customer_id
where live_date between '2023-01-01' and '2023-12-31';
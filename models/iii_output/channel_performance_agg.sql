with channel_performance as (
    select sa.customer_id, case when st.country is null then 'unknown' else st.country end as country, sa.channel, f.five_year_ltv, f.cac
    from {{ ref(sales) }} sa
    left join {{ ref(store) }} st on sa.customer_id = st.customer_id
    left join {{ ref(financials) }} f on sa.customer_id = f.customer_id;
)

select country, channel, count(*) as order_count, sum(five_year_ltv) as total_ltv, sum(cac) as total_cac, sum(five_year_ltv) - sum(cac) as total_ltv_after_cac,
from channel_performance
group by 1,2
order by 1,2;

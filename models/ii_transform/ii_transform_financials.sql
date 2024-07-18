with raw_financials as (
    select * from {{ source('s1_input', 'RAW_FINANCIALS') }}
),

final as (
    select * from raw_financials
)

select * from final;
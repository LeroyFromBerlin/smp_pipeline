/* There is one customer_id that has been acquired by 2 sales reps (in different markets). 
I'll assume that these should be treated as separate acquisitions and both agents will be credited with the sale. 
Keep as is for now */ 

select * from {{ source('S1_INPUT', 'RAW_FUNNEL') }}

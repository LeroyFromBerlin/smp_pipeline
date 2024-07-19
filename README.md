# Overall steps

1) create a Snowflake test account:
- https://vhvyxue-ts21052.snowflakecomputing.com
- LeroyJacob
- Gismo890!

2) dump CSVs into Snowflake

3) do some very basic cleaning to bring the data into a manageable form (column naming, removal of null rows and columns). This happened locally and I added to queries to the model i_init. This model does not have a yml file as I ran this part locally.

4) initialize DBT and transform the data in such a way that the final output can be fetched in a 1- or 2-line query locally from Snowflake at a later stage. This happens in the two models ii_transform and iii_output

I ran into a permission issue when running dbt with DBT struggling to access the Snowflake db. The models I created make sense to me and should work but I did not have the time to debug this and actually test them. Because of that I ran the queries locally in Snowflake to have something presentable.

5) Answer the questions

# Models

### i_init.sql

This creates 4 tables in the schema RAW_DATA.PUBLIC.

### ii_transform.sql

This model transforms the tables in such a way that they can be further processed in later stages. It's primarily about ensuring clean joins in between the tables in the latest step, that is making sure that customer_id can be used to connect the tables.

Assumptions:

1) There are customer_id's that have different financials along with different values for device_id. Example: customer_id = 100. Device_id could be a way of linking to different financials in different markets but it does not show in any of the other tables. There is no way for me to know which row to use in such cases. I'm making the decision to use an average and ony keep one row per customer_id to avoid issues with joining further down the pipeline.

2) There is one customer_id that has been acquired by 2 sales reps (in different markets). I'll assume that these should be treated as separate acquisitions and both agents will be credited with the sale. 

3) There are customers that have been acquired multiple times (in line with the funnel table). I assume that each row represents a separate market. Since the data is incomplete with rows and columns missing, I can't join this to the right funnel because of the missing country column. I'll only keep the first acquisition per customer_id to enable a 1-on-1 join in later steps and drop the later rows. There turns out to be 1 customer (cid 194) that has two entries with the very same acquisition_date. I'll leave it in for now but keep it on the radar to decide later if that poses a problem.

4) There is a bunch of customer_id's that have separate entries for separate countries and typologies. I'll assume that there should only be one typology per customer_id in a given market and keep the latest one, essentially treating this as a typology change. I do keep all the markets, though

### iii_output.sql

Tables are joined and brought into a form that allows for short and simple queries to answer the initial questions.

# Answering the questions

I'm running this part locally in Snowflake as well based on the tables I created in the s2_transformation schema.

### 1. Which acquisition channel performed the best in each country in 2023?

-- based on ltv after cac direct sales is always the best
``` sql
select country, channel, total_ltv_after_cac from
(select country, channel, total_ltv_after_cac, ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_ltv_after_cac desc) AS rank
from S3_OUTPUT.channel_performance_agg) where rank = 1;
```

-- based on order count direct sales is always on top except France (web)
``` sql
select country, channel, order_count from
(select country, channel, order_count, ROW_NUMBER() OVER (PARTITION BY country ORDER BY order_count desc) AS rank
from S3_OUTPUT.channel_performance_agg) where rank = 1;
```

### 2. Which acquisition channel performed the worst in each country in 2023?

-- based on ltv after cac web is always the worst
``` sql
select country, channel, total_ltv_after_cac from
(select country, channel, total_ltv_after_cac, ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_ltv_after_cac asc) AS rank
from S3_OUTPUT.channel_performance_agg) where rank = 1;
```

-- retail is at the bottom in FR, UK and web is in DE, IT, unknown
``` sql
select country, channel, order_count from
(select country, channel, order_count, ROW_NUMBER() OVER (PARTITION BY country ORDER BY order_count asc) AS rank
from S3_OUTPUT.channel_performance_agg) where rank = 1;
```

### 3. Who were the top 3 best performing sales reps in 2023?

top 3
- archie.woods@sumup.com
- candice.laurent@sumup.com
- maxime.melone@sumup.com

--> It's because of the number of sales, not because they have higher ltv after cac per sale.

-- absolute and relative LTV after CAC
``` sql
select sales_email, count(*) as count_orders, sum(five_year_ltv) as total_5yr_ltv, sum(cac) as total_cac, sum(ltv_after_cac) total_ltv_after_cac, sum(ltv_after_cac)/count(*) as relative_ltv_after_cac
from S3_OUTPUT.sales_rep_performance group by 1 order by 5 desc;
```

### 4. How long was the average lead to live cycle, overall and by top 5 typologies?

I'm assuming that top typology is defined as one with the most orders. When I run the query I notice that 4 customers tie for the ranks 4-7. I'll allow for Snowflake to randomly pick ranks 4 and 5 when I run my query.

Overall the average time is 148 days in between lead creation and go-live. For the top5 typologies the range is between 125 and 158 days.

-- overall
``` sql
select avg(daydelta) from S3_OUTPUT.funnel_duration;
```

-- by typology
``` sql
select typology, max(is_top_5) as is_top_5, max(rank) as rank, avg(daydelta) from S3_OUTPUT.funnel_duration group by 1 order by rank asc limit 5;
```

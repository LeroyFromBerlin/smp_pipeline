DBT project name: smp_pipeline 

Snowflake test account creds:
- https://vhvyxue-ts21052.snowflakecomputing.com
- LeroyJacob
- Gismo890!



i_init.sql

I used a Snowflake trial account for this. The four CSVs are loaded into Snowflake manually and I did some basic cleaning
Column naming
Removal of null rows and columns 
This would normally already be part of the pipeline maintenance but I wanted to move fast and did this part manually.

This creates 4 tables in the schema RAW_DATA.PUBLIC.

ii_transform.sql

Assumptions:
The Financials table has a couple of customers that have multiple rows with LTV/CAC for different device_id’s. Device_id would probably allow me to map each entry to a market but it does not show in any other table. I’ll assume that it’s a reasonable guess to use the average LTV and CAC for all markets a customer operates in.
I’m assuming that customers in different markets count as individual sales
Latest topology should be used per market

This does something in the schema TRANSFORMED_DATA.PUBLIC.

iii_output.sql

I'm runnint this part locally as well based on the tables I created in the s2_transformation schema.







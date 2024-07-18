
-- inspect base tables and do basic transformations
-- this only has to run a single time!
-- I ran this locally in Snowflake

use role accountadmin;

create warehouse if not exists sumup_wh with warehouse_size='x-small';
create database if not exists sumup_db;
create role if not exists sumup_role;

show grants on warehouse sumup_wh;

grant usage on warehouse sumup_wh to role sumup_role;
grant role sumup_role to user leroyjacob;
grant all on database sumup_db to role sumup_role;
grant all on schema sumup_db.s1_input to role sumup_role;
grant all on schema sumup_db.s2_transformation to role sumup_role;
grant all on schema sumup_db.s3_output to role sumup_role;
grant all on schema sumup_db.s1_input to role accountadmin;
grant all on schema sumup_db.s2_transformation to role accountadmin;
grant all on schema sumup_db.s3_output to role accountadmin;

use role sumup_role;

create schema sumup_db.s1_input;
create schema sumup_db.s2_transformation;
create schema sumup_db.s3_output;

-- 1)
-- financials
SELECT * FROM sumup_db.s1_input.RAW_FINANCIALS LIMIT 10;

ALTER TABLE sumup_db.s1_input.RAW_FINANCIALS RENAME COLUMN C1 TO customer_id;
ALTER TABLE sumup_db.s1_input.RAW_FINANCIALS RENAME COLUMN C2 TO device_id;
ALTER TABLE sumup_db.s1_input.RAW_FINANCIALS RENAME COLUMN C3 TO five_year_ltv;
ALTER TABLE sumup_db.s1_input.RAW_FINANCIALS RENAME COLUMN C4 TO cac;
ALTER TABLE sumup_db.s1_input.RAW_FINANCIALS RENAME COLUMN C5 TO channel;

DELETE FROM sumup_db.s1_input.RAW_FINANCIALS WHERE customer_id = 'id';
DELETE FROM sumup_db.s1_input.RAW_FINANCIALS WHERE customer_id is null;

ALTER TABLE sumup_db.s1_input.RAW_FINANCIALS DROP COLUMN C6, C7, C8, C9, C10, C11, C12, C13;

-- 2)
-- funnel
SELECT * FROM sumup_db.s1_input.RAW_FUNNEL LIMIT 10;

-- 3)
-- sales
SELECT * FROM sumup_db.s1_input.RAW_SALES LIMIT 10;

ALTER TABLE sumup_db.s1_input.RAW_SALES RENAME COLUMN C1 TO customer_id;
ALTER TABLE sumup_db.s1_input.RAW_SALES RENAME COLUMN C2 TO channel;
ALTER TABLE sumup_db.s1_input.RAW_SALES RENAME COLUMN C3 TO acquisition_month;

DELETE FROM sumup_db.s1_input.RAW_SALES WHERE CUSTOMER_ID = 'customer_id';
DELETE FROM sumup_db.s1_input.RAW_SALES WHERE CUSTOMER_ID is null;

ALTER TABLE sumup_db.s1_input.RAW_SALES ADD COLUMN ACQUISITION_DATE DATE;
UPDATE sumup_db.s1_input.RAW_SALES SET ACQUISITION_DATE = ACQUISITION_MONTH;

ALTER TABLE sumup_db.s1_input.RAW_SALES DROP COLUMN C4, C5, C6, C7, C8, ACQUISITION_MONTH;

-- 4)
-- stores
SELECT * FROM sumup_db.s1_input.RAW_STORE LIMIT 10;

ALTER TABLE sumup_db.s1_input.RAW_STORE RENAME COLUMN C1 TO customer_id;
ALTER TABLE sumup_db.s1_input.RAW_STORE RENAME COLUMN C2 TO country;
ALTER TABLE sumup_db.s1_input.RAW_STORE RENAME COLUMN C3 TO created_date;
ALTER TABLE sumup_db.s1_input.RAW_STORE RENAME COLUMN C4 TO typology;

DELETE FROM sumup_db.s1_input.RAW_STORE WHERE CUSTOMER_ID = 'customer id';
DELETE FROM sumup_db.s1_input.RAW_STORE WHERE CUSTOMER_ID is null;

UPDATE sumup_db.s1_input.RAW_STORE SET CREATED_DATE = left(CREATED_DATE, 10); -- only keep date

ALTER TABLE sumup_db.s1_input.RAW_STORE ADD COLUMN CREATED_AT DATE;
UPDATE sumup_db.s1_input.RAW_STORE SET CREATED_AT = try_to_date(CREATED_DATE);

ALTER TABLE sumup_db.s1_input.RAW_STORE DROP COLUMN C5, C6, C7, C8, C9, C10, C11, C12, CREATED_DATE;

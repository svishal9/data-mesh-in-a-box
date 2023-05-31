-- show syntax error
-- show dependency error
-- select table from partially right script
-- select from information_schema.tables/views
-- show output from myschema.flyway_schema_history
create view cust.test_view_customer_address as
    select *
    FROM
    cust.address
;

create view cust.test_view_customer as
select distinct customer_id
           FROM
    cust.customer
;

-- try altering previous script to get rid of load_data column and show error
-- now copy v104
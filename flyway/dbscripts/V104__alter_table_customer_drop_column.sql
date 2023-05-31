-- try altering the previous script before uncommenting following
drop view cust.test_view_customer;

alter table cust.address drop column zipcode cascade;

create view cust.test_view_customer as
select *
FROM
    cust.address
;


-- select following table after failed migration
-- once created successfully, in which schema would following table go?
-- show output from myschema.flyway_schema_history
create table test (
                      address_id NUMERIC CONSTRAINT address_key PRIMARY KEY,
                      addresstype CHARACTER VARYING(5),
                      member_id NUMERIC,
                      status CHARACTER VARYING(1),
                      address1 CHARACTER VARYING(256),
                      address2 CHARACTER VARYING(128),
                      address3 CHARACTER VARYING(128),
                      city CHARACTER VARYING(128),
                      state CHARACTER VARYING(128),
                      country CHARACTER VARYING(128),
                      zipcode CHARACTER VARYING(40),
                      load_date DATE
)
;

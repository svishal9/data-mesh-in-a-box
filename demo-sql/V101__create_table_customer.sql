-- show ordering of statements
-- show syntax error
-- talk about versioning, naming conventions and possible process
-- select from table after script is partially right/succeeds
-- show output from myschema.flyway_schema_history
-- show output from select * from information_schema.tables where table_schema='cust';
CREATE TABLE IF NOT EXISTS custa.address(
                                            address_id INT GENERATED ALWAYS AS IDENTITY,
                                            addresstype CHARACTER VARYING(5),
                                            customer_id NUMERIC,
                                            status CHARACTER VARYING(1),
                                            address1 CHARACTER VARYING(256),
                                            address2 CHARACTER VARYING(128),
                                            address3 CHARACTER VARYING(128),
                                            city CHARACTER VARYING(128),
                                            state CHARACTER VARYING(128),
                                            country CHARACTER VARYING(128),
                                            zipcode CHARACTER VARYING(40),
                                            PRIMARY KEY(address_id),
                                            CONSTRAINT fk_customer
                                                FOREIGN KEY(customer_id)
                                                    REFERENCES customer(customer_id)
)
;

CREATE TABLE IF NOT EXISTS cust.customer(
                                            customer_id NUMERIC CONSTRAINT address_key PRIMARY KEY,
                                           status CHARACTER VARYING(1),
                                           name CHARACTER VARYING(256),
                                           title CHARACTER VARYING(128),
                                           phone CHARACTER VARYING(128),
                                           load_date DATE
)
;
-- show output from select * from information_schema.tables where table_schema='cust';
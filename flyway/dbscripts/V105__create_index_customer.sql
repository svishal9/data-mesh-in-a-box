-- before creating following try altering create table script
-- CREATE INDEX idx_address_cust_id ON custa.address ( customer_id, address_id);
-- ;

-- don't fix error in above statement instead comment it
-- show output from myschema.flyway_schema_history
-- talk about placeholders and uncomment following

CREATE INDEX idx_address_cust_id ON ${customer_schema}.address (customer_id,address_id);
;

-- show following output
/*
SELECT
    tablename,
    indexname,
    indexdef
FROM
    pg_indexes
WHERE
    schemaname = 'cust' and tablename='address'
ORDER BY
    tablename,
    indexname;

 */

# Assumptions

Assume you have the following table in a PostgreSQL database
```sql
CREATE TABLE app_user_visits_fact (
id text NOT NULL,
phone_number text NULL,
seen int4 NULL,
state int4 NULL,
points float8 NULL,
receipt float8 NULL,
"countryCode" text NULL,
remaining float8 NULL,
customer_id text NOT NULL,
branch_id text NOT NULL,
store_id text NOT NULL,
cashier_id text NOT NULL,
created_at int8 NULL,
updated_at int8 NULL,
expired int4 NULL,
expires_at int8 NULL,
order_id text NULL,
is_deleted int2 DEFAULT '0'::smallint NOT NULL,
is_fraud int2 DEFAULT '0'::smallint NOT NULL,
sync_mechanism text NULL,
is_bulk_points text NULL,
CONSTRAINT app_user_visits_fact_pkey PRIMARY KEY (id),
CONSTRAINT app_user_visits_fact_branch_id_foreign FOREIGN KEY (branch_id) REFERENCES public.branches(id) ON DELETE CASCADE,
CONSTRAINT app_user_visits_fact_cashier_id_foreign FOREIGN KEY (cashier_id) REFERENCES public.cashiers(id) ON DELETE CASCADE,
CONSTRAINT app_user_visits_fact_customer_id_foreign FOREIGN KEY (customer_id) REFERENCES public.app_users(id) ON DELETE CASCADE,
CONSTRAINT app_user_visits_fact_store_id_foreign FOREIGN KEY (store_id) REFERENCES public.stores(id) ON DELETE CASCADE
)

WITH (
autovacuum_vacuum_scale_factor=0.05,
autovacuum_vacuum_cost_delay=2
);
```

# ask

Create an Apache Spark application that reads newly inserted records into this table and inserts them into an identical ClickHouse table. 
The Spark application is expected to run every 30 mins. 
You are free to create the spark application using structured streaming and a CDC driver for postgres, 
or batch reading/writing and rely on the source tables created_at/updated_at to detect new records.

Provide a DDL script for the newly created table on ClickHouse. 
Provide any additional configurations needed for Apache Spark to be able to connect to Postgres or ClickHouse.

You can find sample data for you to test your code within the same archive

# Deliverables

- Working Apache Spark application written in Python
- DDL script for new table creation in ClickHouse
- Any other configuration needed for Spark to be able to connect to Postgres or ClickHouse

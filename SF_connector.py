import snowflake.connector

conn =snowflake.connector.connect(
    user="Samaish1507",
    password="Aashay08112005",
    account="ZVATBPV-FE87133",  # e.g. abcd-xy12345
    warehouse="COMPUTE_WH",
    database="Aish_SQL_SF",
    schema="PUBLIC"
)
cs = conn.cursor()
print("Connection successful!")

cs.execute("""
CREATE OR REPLACE TABLE customers (
    id INT,
    name STRING,
    email STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
""")

cs.execute("CREATE OR REPLACE STAGE my_stage")

cs.execute("PUT file://customers.csv @my_stage")

cs.execute("""
COPY INTO customers
FROM @my_stage/customers.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
""")
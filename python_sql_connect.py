import pandas as pd
from sqlalchemy import create_engine

# MySQL connection (localhost)
mysql_engine = create_engine(
    "mysql+pymysql://",   # no need to embed creds here
    connect_args={
        "user": "root",
        "password": "NewStrongPassword!",  # safe here
        "host": "localhost",
        "database": "Aishwarya_Export_To_Snowflake"
    }
)

 # Read table into DataFrame
df = pd.read_sql("select * from customers", mysql_engine)

# Save locally as CSV (staging file)
df.to_csv("customers.csv", index=False)
print("Extracted data from MySQL and saved as customers.csv")

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Expections
customers_rules={
    "valid_customer_id":"customer_id IS NOT NULL",
    "valid_customer_name":"customer_name IS NOT NULL",
    "valid_phone_length": "LENGTH(phone_number) >= 10"
}

#Ingesting customers
@dlt.table(
    name="customers_stg"
)
@dlt.expect_all_or_drop(customers_rules)
def customers_stg():
    df=spark.readStream.table("`e-commerce-project`.source_dimensional_tables.customers")
    return df


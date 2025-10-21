import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Expectations (data quality rules)
products_rules = {
    "valid_product_id": "product_id IS NOT NULL",
    "valid_product_name": "product_name IS NOT NULL",
    "valid_price": "price >= 0",
    "valid_category": "product_category IS NOT NULL"
}

# Ingesting products
@dlt.table(
    name="products_stg"
)
@dlt.expect_all_or_drop(products_rules)
def products_stg():
    df = spark.readStream.table("`e-commerce-project`.source_dimensional_tables.products")
    return df

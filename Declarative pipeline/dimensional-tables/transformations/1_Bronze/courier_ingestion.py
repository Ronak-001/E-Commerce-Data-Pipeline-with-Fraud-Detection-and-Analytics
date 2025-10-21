import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Expectations (data quality rules)
courier_rules = {
    "valid_courier_id": "courier_id IS NOT NULL",
    "valid_courier_name": "courier_name IS NOT NULL",
    "valid_service_type": "service_type IS NOT NULL",
    "india_only_service":"lower(region) = 'india'"
}

# Ingesting courier partners
@dlt.table(
    name="courier_partners_stg"
)
@dlt.expect_all_or_drop(courier_rules)
def courier_partners_stg():
    df = spark.read.table("`e-commerce-project`.source_dimensional_tables.courier_partners")
    return df

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Create empty streaming table
dlt.create_streaming_table(
    name="`e-commerce-project`.gold.dim_products"
)
#AUTO-CDC flow
dlt.create_auto_cdc_flow(
  target = '`e-commerce-project`.gold.dim_products',
  source = "products_transformed_table",
  keys = ["product_id"],
  sequence_by = "last_updated",
  stored_as_scd_type = "2"
)



# import dlt
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# #Create empty streaming table

# dlt.create_streaming_table(
#     name="`dlt-declarative_pipelines`.source.dim_products"
# )
# #AUTO-CDC flow
# dlt.create_auto_cdc_flow(
#   target = "`dlt-declarative_pipelines`.source.dim_products",
#   source = "customers_transformed_table",
#   keys = ["customer_id"],
#   sequence_by = "last_updated",
#   stored_as_scd_type = "2"
# )






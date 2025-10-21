import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utilities.utils import Transformations


#  customers enriched view
@dlt.view(
    name="products_enriched_view"
)
def customers_enriched_view():
    df=spark.readStream.table("products_stg")
    return df

#applying transformations
@dlt.table(
    name="products_transformed_table"
)
def customers_transformed_table():
    df=dlt.read_stream("products_enriched_view")
    df = (
        df.withColumn("product_name", initcap(col("product_name")))# Clean name format 
          .withColumn("brand", upper(col("brand")))# Brand in uppercase for consistency
    )
    df=df.dropDuplicates()
    util =Transformations(df)
    df=util.add_timestamp()
    return df
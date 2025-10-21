import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utilities.utils import Transformations

#  customers enriched view
@dlt.view(
    name="customers_enriched_view"
)
def customers_enriched_view():
    df=spark.readStream.table("customers_stg")
    return df

#applying transformations
@dlt.table(
    name="customers_transformed_table"
)
def customers_transformed_table():
    df=dlt.read_stream("customers_enriched_view")
    df = (
        df.withColumn("customer_name",lower("customer_name"))
          .withColumn("customer_name",initcap("customer_name"))
    )
    df=df.dropDuplicates()
    util =Transformations(df)
    df=util.add_timestamp()
    return df
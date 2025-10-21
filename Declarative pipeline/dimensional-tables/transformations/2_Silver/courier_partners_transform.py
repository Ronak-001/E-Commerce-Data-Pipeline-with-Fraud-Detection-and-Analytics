import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

from utilities.utils import Transformations
#  customers enriched view


@dlt.view(
    name="courier_partners_enriched_view"
)
def customers_enriched_view():
    df=spark.readStream.table("courier_partners_stg")
    return df

#applying transformations
@dlt.table(
    name="courier_partners_transformed_table"
)
def customers_transformed_table():
    df=dlt.read_stream("courier_partners_enriched_view")
    df = (
        df.withColumn("courier_name", initcap(col("courier_name")))  # Normalize courier names
          .withColumn("service_type", upper(col("service_type")))     # Standardize service type to uppercase
    )
    
    df=df.dropDuplicates()
    util =Transformations(df)
    df=util.add_timestamp()
    return df
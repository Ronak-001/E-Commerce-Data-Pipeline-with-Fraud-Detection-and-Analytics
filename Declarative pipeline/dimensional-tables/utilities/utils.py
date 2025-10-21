from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

class Transformations: 
    def __init__(self,df):
        self.df=df  
    def add_timestamp(self):
        self.df=self.df.withColumn("last_updated",current_timestamp())
        return self.df
    # return 1
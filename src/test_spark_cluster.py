from pyspark.sql.functions import when
from pyspark.sql.functions import current_date
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession


spark = None
if False:
    spark = SparkSession.builder \
                        .appName('dasdasd') \
                        .getOrCreate()


if False:
    spark = SparkSession \
        .builder \
        .master("spark://4f6cf4ef8fad:7077") \
        .appName('Test with IP') \
        .getOrCreate()
# .master("spark://192.168.128.3:7077") \

if True:
    spark = SparkSession \
        .builder \
        .master("spark://spark-master:7077") \
        .appName('TestwithIP') \
        .getOrCreate()
# .master("spark://192.168.128.3:7077") \

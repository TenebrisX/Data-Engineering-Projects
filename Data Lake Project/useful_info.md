```
! hdfs dfs -copyFromLocal /lessons/geo.csv /user/kotlyarovb/sm_data_lake_project/staging/geo

```


```
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3'

import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
spark = SparkSession \
    .builder \
    .master("yarn") \
    .config("spark.driver.cores", "2") \
    .config("spark.driver.memory", "1g") \
    .appName("sm_data_lake_project") \
    .getOrCreate()
    
```

```
df = sql.read.parquet("/user/master/data/geo/events/").sample(0.25)

df.repartition(12) \
    .write \
    .mode('overwrite') \
    .partitionBy("event_type") \
    .parquet("/user/kotlyarovb/sm_data_lake_project/staging/events")
```
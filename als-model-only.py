from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline 
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import numpy
from functools import reduce
from pyspark.sql import DataFrame

SparkContext.setSystemProperty('spark.driver.memory', '1g')
SparkContext.setSystemProperty('spark.driver.cores', '4')
SparkContext.setSystemProperty('spark.executor.memory', '2g')
SparkContext.setSystemProperty('spark.executor.cores', '4')

spark = SparkSession.builder.appName("Movies: REC MODEL") \
    .config("dfs.client.read.shortcircuit.skip.checksum", "true") \
    .getOrCreate()
sc = spark.sparkContext

sc.setCheckpointDir('/ML/movies/checkpoint/')

log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.info("############################## PYSPARK LOG")

df = spark.read.load(path='/ML/movies/data/reatings/*',
                          format='com.databricks.spark.csv', 
                          delimiter=',',
                          inferSchema='true',
                          header="true")

df = df.drop('timestamp')
oldColumns = df.schema.names
newColumns = ["userId", "itemId", "rating"]
df = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx], newColumns[idx]),range(len(oldColumns)),df)

df = df.withColumn("userId", df["userId"].cast("string"))
df = df.withColumn("itemId", df["itemId"].cast("string"))

indexer_acc = StringIndexer(inputCol="userId", outputCol="userIndex")
indexer_acc_fitted = indexer_acc.fit(df)
df = indexer_acc_fitted.transform(df)

indexer_mer = StringIndexer(inputCol="itemId", outputCol="itemIndex")
indexer_mer_fitted = indexer_mer.fit(df)
df = indexer_mer_fitted.transform(df)

print('############################## - SPLITING THE TRAIN AND TEST DATA')
(training, test) = df.randomSplit([0.9, 0.1], seed=42)

print('############################## - TRAINING THE DATA')
als = ALS(rank=20, maxIter=20,regParam=0.14, userCol="userIndex", itemCol="itemIndex", ratingCol="rating", seed=42)

print('############################## - TESTING DATA')
model = als.fit(training)

print('############################## - TESTING THE DATA')
predictions = model.transform(test)

predictions = predictions.na.drop()
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")

rmse = evaluator.evaluate(predictions)
print("##################### Root-mean-square error = " + str(rmse))

model.save("/ML/movies/models/moviesrec/")


print('############################## - MODEL CREATED SUCCESSFULLY')

# spark-submit als-model-only.py --master yarn --deploy-mode client --executor-memory 2G --num-executors 3



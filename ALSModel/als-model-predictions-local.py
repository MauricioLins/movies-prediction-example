from pyspark import SparkContext, SparkConf
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml import Pipeline
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

SparkContext.setSystemProperty('spark.driver.memory', '2g')
SparkContext.setSystemProperty('spark.driver.cores', '3')
SparkContext.setSystemProperty('spark.executor.memory', '2g')
SparkContext.setSystemProperty('spark.executor.cores', '3')
SparkContext.setSystemProperty('spark.storage.memoryFraction', '0.9')
#SparkContext.setSystemProperty('spark.driver.memoryOverhead', '2g')

sqlContext = SparkSession.builder.config("dfs.client.read.shortcircuit.skip.checksum", "true").getOrCreate()

df = sqlContext.read.load(path='ml-latest/ratings.csv',
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

print('############################## - LOADING MODEL - ##############################')
model = ALSModel.load('models/moviesrec/')

print('############################## - CLASSIFYING DATA')

userRecommends = model.recommendForAllUsers(10)
userRecommends.show(truncate=False)

print('############################## - EXPLODING PREDICTIONS')
flatUserRecomends = userRecommends.withColumn('userAndRatings', explode(userRecommends.recommendations)).select('userIndex','userAndRatings.*')
flatUserRecomends.show(truncate=False)
print('############################## - CONVERTING INDEXES TO STRING')
userConverter = IndexToString(inputCol='userIndex', outputCol='userId', labels=indexer_acc_fitted.labels)
itemConverter = IndexToString(inputCol='itemIndex', outputCol='itemId', labels=indexer_mer_fitted.labels)

convertedMoviesRecs = Pipeline(stages=[userConverter,itemConverter]).fit(df).transform(flatUserRecomends)
print('############################## - SAVING DATA')

convertedMoviesRecs.write.json('results/usersrec/')

#userRecomends.write.format('json').save('/ML/movies/usersrec/')
# spark-submit als-model-predictions.py --master yarn --deploy-mode client --num-executors 2 --driver-java-options "-XX:+UseG1GC -XX:ResizePLAB -Xms1g -Xmx1g -XX:InitiatingHeapOccupancyPercent=35" --conf "spark.sql.tungthen.enabled=true" --conf "spark.serializer=org.apache.spark.serializer.KyrioSerializer" --conf "spark.memory.fraction=0.3" --conf "spark.driver.memoryOverhead=2g" --conf "spark.executor.memoryOverhead=1g" --conf "spark.executor.extraJavaOptions -XX:+UseG1GC -XX:ResizePLAB -Xms3g -Xmx3g -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThread=20"


from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml import Pipeline
from pyspark.sql.functions import explode
from functools import reduce
from pyspark.sql import DataFrame

SparkContext.setSystemProperty('spark.driver.memory', '512m')
SparkContext.setSystemProperty('spark.driver.cores', '1')
SparkContext.setSystemProperty('spark.executor.memory', '2560m') #2560m
SparkContext.setSystemProperty('spark.executor.cores', '8')

#SparkContext.setSystemProperty('spark.executor.memoryOverhead', '1536m')
SparkContext.setSystemProperty("spark.scheduler.mode","FAIR")
SparkContext.setSystemProperty('spark.memory.fraction', '0.8')
SparkContext.setSystemProperty('spark.memory.storageFraction', '0.1')
SparkContext.setSystemProperty("spark.default.parallelism","256")
SparkContext.setSystemProperty("spark.num.executors","1")
SparkContext.setSystemProperty("spark.local.dir","/tmp")


conf = SparkConf().setAppName('MoviesRec: Preditcions')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

sc.setCheckpointDir('/ML/movies/checkpoint/')

df = sqlContext.read.load(path='/ML/movies/data/*', 
                          format='com.databricks.spark.csv', 
                          delimiter=',',
                          inferSchema='true',
                          header="true").cache()

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
model = ALSModel.load('/ML/movies/models/moviesrec/')

print('############################## - CLASSIFYING DATA')

userRecommends = model.recommendForAllUsers(10)

print('############################## - EXPLODING PREDICTIONS')
flatUserRecomends = userRecommends.withColumn('userAndRatings', explode(userRecommends.recommendations)).select('userIndex','userAndRatings.*')

print('############################## - CONVERTING INDEXES TO STRING')
userConverter = IndexToString(inputCol='userIndex', outputCol='userId', labels=indexer_acc_fitted.labels)
itemConverter = IndexToString(inputCol='itemIndex', outputCol='itemId', labels=indexer_mer_fitted.labels)

convertedMoviesRecs = Pipeline(stages=[userConverter,itemConverter]).fit(df).transform(flatUserRecomends)

print('############################## - SAVING DATA')
df.unpersist()
convertedMoviesRecs.cache()
convertedMoviesRecs.show()

convertedMoviesRecs.write.json('/ML/movies/usersrec/')

# spark-submit als-model-predictions.py --master yarn --deploy-mode client --num-executors 3 --driver-java-options "-XX:+UseG1GC -XX:ResizePLAB -Xms1g -Xmx1g -XX:InitiatingHeapOccupancyPercent=35" --conf "spark.sql.tungthen.enabled=true" --conf "spark.serializer=org.apache.spark.serializer.KyrioSerializer" --conf "spark.memory.fraction=0.9" --conf "spark.driver.memoryOverhead=1g" --conf "spark.executor.memoryOverhead=1g" --conf "spark.executor.extraJavaOptions -XX:+UseG1GC -XX:ResizePLAB -Xms3g -Xmx3g -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThread=20" --conf "spark.scheduler.mode=FAIR"

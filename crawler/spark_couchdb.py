from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, LongType
import couchdb
import requests
import json
import ast

SparkContext.setSystemProperty('spark.driver.memory', '1g')
SparkContext.setSystemProperty('spark.driver.cores', '3')
SparkContext.setSystemProperty('spark.executor.memory', '1g')
SparkContext.setSystemProperty('spark.executor.cores', '3')
SparkContext.setSystemProperty('spark.driver.memoryOverhead', '1g')
SparkContext.setSystemProperty('spark.storage.memoryFraction', '0.9')
SparkContext.setSystemProperty('spark.sql.codegen.wholeStage', 'false')

spark = SparkSession.builder.appName("Movies: DATA PREP")\
                            .config("dfs.client.read.shortcircuit.skip.checksum", "true")\
                            .getOrCreate()

print("##################################### LOADING MOVIES DATASET")
### LOAD MOVIES DATASET
movies_df = spark.read.csv('ml-latest/movies.csv',header=True,inferSchema=True) \
    .select('movieId', 'genres').rdd.flatMapValues(lambda x:x.split("|")) \
    .toDF(['IdMovie', 'genres'])

print("##################################### GATHERING DATA FROM COUCHDB")
response = requests.get("http://jarvis3:5984/moviesdetails/_all_docs?include_docs=true")
y        = [response.text][0]

print("##################################### GROUPING DOCUMENTS")
docs = dict(ast.literal_eval(y))['rows']
content_docs = []
for doc in docs:
    content_docs.append(doc['doc'])

print("##################################### CREATING THE DATAFRAME")
json_rdd = spark.sparkContext.parallelize(content_docs)
json_df  = spark.read.json(json_rdd).filter(("BudgetCurrency is not null")).filter(("BudgetCurrency not in('NA')"))

json_df.printSchema()

### UDF TO TRIM STRINGS
replaceSubsUDF = udf(lambda s: s.replace(s[0:2], ''), StringType())

print("##################################### DATA PREPARATION")

### CLEANING BUDGET VALUE
json_df_budget = json_df.withColumn('BudgetValueClean', when((json_df['BudgetCurrency']=='H') & (json_df['BudgetValue'][0:2]=='UF'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='H') & (json_df['BudgetValue'][0:2]=='KD'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='H') & (json_df['BudgetValue'][0:2]=='RK'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='P') & (json_df['BudgetValue'][0:2]=='LN'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='P') & (json_df['BudgetValue'][0:2]=='HP'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='P') & (json_df['BudgetValue'][0:2]=='TE'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='P') & (json_df['BudgetValue'][0:2]=='KR'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='S') & (json_df['BudgetValue'][0:2]=='EK'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='S') & (json_df['BudgetValue'][0:2]=='GD'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='S') & (json_df['BudgetValue'][0:2]=='IT'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='C') & (json_df['BudgetValue'][0:2]=='AD'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='C') & (json_df['BudgetValue'][0:2]=='NY'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='C') & (json_df['BudgetValue'][0:2]=='LP'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='C') & (json_df['BudgetValue'][0:2]=='ZK'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='C') & (json_df['BudgetValue'][0:2]=='OP'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='C') & (json_df['BudgetValue'][0:2]=='HF'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='G') & (json_df['BudgetValue'][0:2]=='BP'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='G') & (json_df['BudgetValue'][0:2]=='RD'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='E') & (json_df['BudgetValue'][0:2]=='UR'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='E') & (json_df['BudgetValue'][0:2]=='SP'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='E') & (json_df['BudgetValue'][0:2]=='EK'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='B') & (json_df['BudgetValue'][0:2]=='RL'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='B') & (json_df['BudgetValue'][0:2]=='EF'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='B') & (json_df['BudgetValue'][0:2]=='ND'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='B') & (json_df['BudgetValue'][0:2]=='GL'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='R') & (json_df['BudgetValue'][0:2]=='UR'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='R') & (json_df['BudgetValue'][0:2]=='OL'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='R') & (json_df['BudgetValue'][0:2]=='ON'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='F') & (json_df['BudgetValue'][0:2]=='IM'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='F') & (json_df['BudgetValue'][0:2]=='RF'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='A') & (json_df['BudgetValue'][0:2]=='UD'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='A') & (json_df['BudgetValue'][0:2]=='RS'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='A') & (json_df['BudgetValue'][0:2]=='TS'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='A') & (json_df['BudgetValue'][0:2]=='MD'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='J') & (json_df['BudgetValue'][0:2]=='PY'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='J') & (json_df['BudgetValue'][0:2]=='MD'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='N') & (json_df['BudgetValue'][0:2]=='ZD'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='N') & (json_df['BudgetValue'][0:2]=='LG'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='N') & (json_df['BudgetValue'][0:2]=='OK'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='N') & (json_df['BudgetValue'][0:2]=='GN'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='D') & (json_df['BudgetValue'][0:2]=='KK'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='D') & (json_df['BudgetValue'][0:2]=='EM'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='I') & (json_df['BudgetValue'][0:2]=='LS'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='I') & (json_df['BudgetValue'][0:2]=='TL'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='I') & (json_df['BudgetValue'][0:2]=='NR'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='I') & (json_df['BudgetValue'][0:2]=='SK'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='I') & (json_df['BudgetValue'][0:2]=='DR'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='I') & (json_df['BudgetValue'][0:2]=='RR'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='I') & (json_df['BudgetValue'][0:2]=='EP'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='T') & (json_df['BudgetValue'][0:2]=='HB'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='T') & (json_df['BudgetValue'][0:2]=='RL'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='T') & (json_df['BudgetValue'][0:2]=='WD'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='Z') & (json_df['BudgetValue'][0:2]=='AR'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='M') & (json_df['BudgetValue'][0:2]=='XN'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='M') & (json_df['BudgetValue'][0:2]=='YR'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='M') & (json_df['BudgetValue'][0:2]=='NT'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='K') & (json_df['BudgetValue'][0:2]=='RW'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='U') & (json_df['BudgetValue'][0:2]=='AH'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='L') & (json_df['BudgetValue'][0:2]=='VL'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='L') & (json_df['BudgetValue'][0:2]=='TL'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='V') & (json_df['BudgetValue'][0:2]=='EB'),replaceSubsUDF(json_df['BudgetValue']))
                                    .when((json_df['BudgetCurrency']=='X') & (json_df['BudgetValue'][0:2]=='AU'),replaceSubsUDF(json_df['BudgetValue']))
                                    .otherwise(json_df['BudgetValue'])).drop('BudgetValue').withColumnRenamed('BudgetValueClean','BudgetValue')


print("##################################### DATA CONVERSION")

### CONVERTING BUDGET VALUE
json_df_budget_cast = json_df_budget.withColumn('BudgetValueCast', regexp_replace(json_df_budget['BudgetValue'],',','').cast(LongType())) \
                                    .withColumn('RuntimeCast', json_df_budget['Runtime'].cast(IntegerType())) \
                                    .drop('BudgetValue').drop('Runtime')\
                                    .drop('BudgetCurrency') \
                                    .withColumnRenamed('BudgetValueCast','BudgetValue') \
                                    .withColumnRenamed('RuntimeCast','Runtime')

### REMOVE NULL VALUES AND FILTER BUDGET VALUES > 0
json_df_budget_filters = json_df_budget_cast.where(json_df_budget_cast['Director'].isNotNull()).filter("BudgetValue > 0")

### CREATING A DENORMALIZED DIRECTORS DATASET
directors_df = json_df_budget_filters.select('IdMovie', 'Director').rdd.flatMapValues(lambda x:x.split(",")).toDF(['IdMovie', 'Director'])

### LOAD MOVIES DATASET
#movies_df = spark.read.csv('/ML/movies/data/movies/*',header=True,inferSchema=True)\
#                                                        .select('movieId', 'genres').rdd.flatMapValues(lambda x:x.split("|"))\
#                                                        .toDF(['IdMovie', 'genres'])

print("##################################### LOADING RATINGS DATASET")
### LOAD RATINGS DATASET
#ratings_df = spark.read.csv('/ML/movies/data/ratings/*',header=True,inferSchema=True).select('movieId', 'rating')\
ratings_df = spark.read.csv('ml-latest/ratings.csv',header=True,inferSchema=True).select('movieId', 'rating') \
                                                                                 .withColumnRenamed('movieId','IdMovie')

ratings_df = ratings_df.groupBy('IdMovie').avg('rating').withColumnRenamed('avg(rating)','rating')

#directors_df.show()

### CREATING THE ANALYTICAL BASE TABLE
full_df = json_df_budget_filters.join(directors_df, json_df_budget_filters.IdMovie == directors_df.IdMovie, 'inner')\
                                .drop(directors_df['IdMovie']).drop(json_df_budget_filters.Director) \
                                .join(ratings_df, json_df_budget_filters.IdMovie == ratings_df.IdMovie, 'inner')\
                                .drop(ratings_df['IdMovie']) \
                                .join(movies_df, json_df_budget_filters.IdMovie == movies_df.IdMovie, 'inner') \
                                .drop(movies_df['IdMovie'])\
                                .selectExpr('IdMovie as idmovie',\
                                            'Runtime as runtime',\
                                            'Director as director',\
                                            'genres',
                                            'round(rating, 2) as rating',\
                                            'BudgetValue as budgetvalue')

#full_df.printSchema()
#print('############### TOTAL: ' + str(full_df.count()))
#full_df.show(truncate=False)

#full_df.coalesce(1).write.csv('abt/data/',header=True)

print("##################################### SINK DATA INTO HDFS")
full_df.write.mode('overwrite').parquet('/ML/movies/data/dataprep/')

# spark-submit spark_couchdb.py --master yarn --deploy-mode client --num-executors 3
import os
import json
import logging
from pyspark.ml.regression import GBTRegressor, GBTRegressionModel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BudgetPredictionEngine:

    def __prediction(self, df_pred):
        logger.info("__prediction: LOADING PIPELINE ##################################### ")
        gbt_pipeline        = Pipeline.load('budget_prediction_pipeline/')
        gbt_pipeline_loaded = gbt_pipeline.fit(df_pred)
        ddf_features_df     = gbt_pipeline_loaded.transform(df_pred)

        logger.info("__prediction: FILTERING DATA ##################################### ")
        ddf_features_df     = ddf_features_df.filter("idmovie in(99999)")

        logger.info("__prediction: LOADING MODEL ##################################### ")
        gbt_model_load  = GBTRegressionModel.load('gbt_model_old/')
        gbt_model_pred  = gbt_model_load.transform(ddf_features_df)
        gbt_model_pred.selectExpr('idmovie','director','genres', 'runtime','cast(prediction as Decimal(38,2)) as prediction').show(truncate=False)

        logger.info("__prediction: DATA PREDICTED ##################################### ")
        return gbt_model_pred.selectExpr('director','genres', 'runtime','prediction')


    def get_prediction(self, spark, director, genre, rating, data_df):
        logger.info("receive_data: FORMAT THE INPUT ##################################### ")

        data_dict = {"idmovie":99999,"runtime":90,"director": str(director) , "genres": str(genre),"rating":rating,"budgetvalue":0}

        djson = [json.dumps(data_dict)]
        dRDD  = spark.sparkContext.parallelize(djson)

        schema = StructType([StructField("idmovie", IntegerType(), True),
                             StructField("runtime", IntegerType(), True),
                             StructField("director", StringType(), True),
                             StructField("genres", StringType(), True),
                             StructField("rating", DoubleType(), True),
                             StructField("budgetvalue", LongType(), True),
                             ])
        logger.info("receive_data: INPUT DATAFRAME CREATION ##################################### ")
        ddf = spark.read.json(dRDD, schema=schema)

        #logger.info("receive_data: LOADING ABT DATA ##################################### ")
        #data_df = self.__loadABT(spark)

        logger.info("receive_data: UNION DATAFRAMES ##################################### ")
        ddf_union = ddf.unionAll(data_df)

        logger.info("receive_data: SENDING TO PREDICTION ##################################### ")
        data_result = self.__prediction(ddf_union)

        return data_result.rdd.collect()


    def loadABT(self, spark):
        logger.info("loadABT: LOADING ABT DATA ##################################### ")

        data_df = spark.read.csv('abt/data/*',header=True, inferSchema=True)
        data_df.cache()

        logger.info("loadABT: ABT LOADED ##################################### ")
        return data_df


    def __init__(self, spark):
        logger.info("__init__: INITIALIZING ENGINE ##################################### ")

        #self.__loadABT(spark)


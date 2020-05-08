import time, sys, cherrypy, os
from srvapp import create_app
from paste.translogger import TransLogger
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

def init_spark_context():

    # load spark context
    #conf = SparkConf().setAppName("movie_recommendation-server")
    # IMPORTANT: pass aditional Python modules to each worker
    #sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])

    SparkContext.setSystemProperty('spark.driver.memory', '1g')
    SparkContext.setSystemProperty('spark.driver.cores', '3')
    SparkContext.setSystemProperty('spark.executor.memory', '1g')
    SparkContext.setSystemProperty('spark.executor.cores', '3')
    SparkContext.setSystemProperty('spark.driver.memoryOverhead', '1g')
    SparkContext.setSystemProperty('spark.storage.memoryFraction', '0.9')

    spark = SparkSession.builder.appName("BGT PREDICTION API") \
        .config("dfs.client.read.shortcircuit.skip.checksum", "true") \
        .getOrCreate()

    spark.sparkContext.addPyFile('srvengine.py')
    spark.sparkContext.addPyFile('srvapp.py')

    return spark

def run_server(app):

    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)

    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')

    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()

if __name__ == "__main__":
    # Init spark context and load libraries
    spark = init_spark_context()
    dataset_path = os.path.join('datasets', 'ml-latest')
    app = create_app(spark, dataset_path)

    # start web server
    run_server(app)


# http://localhost:5432/movie/director/Alfred Hitcock/genre/Crime/rating/3.5
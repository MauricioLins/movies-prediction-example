from flask import Blueprint
main = Blueprint('main', __name__)
import decimal
import json
from srvengine import BudgetPredictionEngine

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request

# http://localhost:5432/movie/director/John Lasseter/genre/Comedy/rating/2.5
@main.route("/movie/director/<director>/genre/<genre>/rating/<float:rating>", methods=["GET"])
def movie_budget_prediction(director, genre, rating=4):
    logger.debug("STARTING PREDICTION FOR DIRECTOR: %s - GENRE: % - RATING: %", director, genre, rating)
    prediction = budget_prediction.get_prediction(sparkCtx, director, genre, rating, df)
    return json.dumps(prediction, default=decimal_default)


def create_app(spark, dataset_path):
    global budget_prediction
    global sparkCtx
    global df

    sparkCtx = spark

    logger.info("create_app: CREATING  BudgetPredictionEngine ##################################### ")
    budget_prediction = BudgetPredictionEngine(spark)
    df                = budget_prediction.loadABT(spark)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)

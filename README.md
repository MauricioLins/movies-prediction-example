# Spark ML Movies Recommendation and Budget Forecast


## About
A demo Python application with web-scrapping/crawler, Kafka, couchdb and SparkML using the famous Movielens dataset using Raspberry Pi Cluster.


<img src="images/flow.png" alt="Demo Architecture"/>

## Instructions

1. To the Recommendation Model with Spark was used the MoviesLens data set (ml-latest), you can download it <a href='https://grouplens.org/datasets/movielens/'>here</a>.
2. The crawler uses the links file to get more data from the IMDB website.
3. For the prediction example the used features are Director, Rating, Budget where some of this data are collected from the crawler.
4. You can find the training notebook with the models GBTRegressor, RandomForestRegressor, DecisionTreeRegressor with Spark ML
5. The API is using Spark to generate the predictions, it needs to run a spark-submit you can find more details in this example <a href="https://www.codementor.io/@jadianes/building-a-web-service-with-apache-spark-flask-example-app-part2-du1083854">here</a>

6. All the used references as support are mentioned in the presentation (ppt)

### API Execution

```spark-submit
spark-submit srvserver.py --master yarn --deploy-mode client --num-executors 3
``` 

## Enjoy! :)
 
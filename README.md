# Spark ML Movies Recommendation and Budget Forecast

<img src="images/flow.png" alt="Demo Architecture"/>

This is a demo Python application with web-scrapping/crawler, Kafka, couchdb and SparkML using the famous Movielens dataset. The used environment for it was Raspberry Pi Cluster with Hadoop, Spark and other technologies.
</br>
In this project you will find two pipelines:

1. **The Recommendation Pipeline:** That is the standard movies recommendation example using the MovieLens databse with Spark ML.
2. **The Budget Prediction Pipeline:** That uses the links.csv file to crawl additional data from movies into the IMDB website as Directors, Budget Value, etc. In this pipeline you will find:
        
      * A python scrapper that collect these data from the IMDB website and store each message into a Kafka queue, with the following format:
      ```
      {'Director': 'Hiner Saleem', 'Writers': 'Hiner Saleem,Antoine Lacomblez', 'Reviews': '14', 'Critic': '37', 'Country': 'Iraq|France|Germany', 'Language': 'Kurdish|Arabic|Turkish', 'BudgetCurrency': 'EUR', 'BudgetValue': '2,600,000', 'Runtime': '100', 'Actors': '', 'IdMovie': '127244', 'IdIMDB': '2875926', 'GrossCurrency': 'NA', 'GrossValue': 0, 'OpeningWeekendCurrency': 'NA', 'OpeningWeekendValue': 0} 
      ```
      * A Kafka consumer that sink the data into a CouchDB database (python script);
      * A Spark process that consume these data to apply a data preparation and create an ABT (Analytical Base Table);
      * A Spark ML pipeline to training the models GBTRegressor, RandomForestRegressor, DecisionTreeRegressor for comparison.
      * Example in how to expose a Spark model as a REST API, in this case using Spark itself to generate the reponses;


**Important:** All the used references as support are mentioned in the presentation (ppt folder)

## The Dataset

You can download it <a href='https://grouplens.org/datasets/movielens/'>here</a>.


### API
The API is using Spark to generate the predictions, so it needs to run a spark-submit.
   * Reference:
        * You can find more details in this very good example, that was used as baseline: <a href="https://www.codementor.io/@jadianes/building-a-web-service-with-apache-spark-flask-example-app-part2-du1083854">Building a Movie Recommendation Service with Apache Spark & Flask - Part 2</a>
   
   * How to execute:
   
```spark-submit
spark-submit srvserver.py --master yarn --deploy-mode client --num-executors 3
``` 

<br>

#### Enjoy! :)
 
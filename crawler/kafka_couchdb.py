from kafka import KafkaConsumer
from crawler import logs
import ast
import couchdb
import datetime as dt

def insertCouchDB():

    print('STARDED: ' + str(dt.datetime.now()))
    try:
        couch = couchdb.Server('http://<USER>:<PASSWORD>@HOST:5984/')
        db    = couch['movies']
    except Exception as e:
        logs.couchDBLog('ERROR: ', str(e), 'err')
        print('TRYING TO CALL insertCouchDB() AGAIN.')
        insertCouchDB()
        pass
    consumer = KafkaConsumer('moviesdata', group_id='movie', bootstrap_servers=['KAFKA-MACHINE:9092'], auto_offset_reset='earliest')

    for message in consumer:
        print(consumer)
        try:
            content = ast.literal_eval(message.value .decode('utf-8'))
            db.save(content)

        except Exception as e:

            logs.couchDBLog('ERROR: ', str(e), 'err')
            pass
    print('ENDED: ' + str(dt.datetime.now()))

if __name__ == '__main__': insertCouchDB()
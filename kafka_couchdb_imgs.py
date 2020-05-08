from kafka import KafkaConsumer
import logs
import ast
import couchdb
import datetime as dt

def insertCouchDB():

    print('STARDED: ' + str(dt.datetime.now()))
    try:
        couch = couchdb.Server('http://admin:h4doop@jarvis3:5984/')
        db    = couch['moviesimages']
    except Exception as e:
        logs.couchDBLog('ERROR: ', str(e), 'err')
        print('TRYING TO CALL insertCouchDB() AGAIN.')
        insertCouchDB()
        pass
    consumer = KafkaConsumer('moviesimages', group_id='mimages', bootstrap_servers=['jarvis2:9092'], auto_offset_reset='earliest')

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

# FROM /home/pi/scrapper/movies/python
# nohup python3 kafka_couchdb_imgs.py  > nohup_imgs.out
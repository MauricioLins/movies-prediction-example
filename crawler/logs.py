import logging
import datetime
import platform

dataDefault = str(datetime.datetime.date(datetime.datetime.now()))

############################################################
############# CRAWLER LOGS
############################################################
def crawlerLog(file,msg,tp):

    file = file.split('/')[-1]

    if(platform.system() == 'Darwin'):
        location_log     = 'logs/crawler_log.log'
        location_log_err = 'logs/crawler_log.log'
    else:
        location_log = r'/home/pi/scrapper/movies/python/logs/crawler_log_'+str(dataDefault)+'-'+file+'.log'
        location_log_err = r'/home/pi/scrapper/movies/python/logs/crawler_log_err_'+str(dataDefault)+'-'+file+'.log'

    if(tp == 'err'):
        logger_err = logging.getLogger(file)
        hdlr = logging.FileHandler(location_log_err)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger_err.addHandler(hdlr)
        logger_err.setLevel(logging.INFO)
        logger_err.error(msg)
    else:
        with open(location_log,mode='a',newline='') as f:
            f.write(str(msg+"\n"))

############################################################
############# KAFKA LOGS
############################################################
def kafkaLog(file,msg,tp):

    file = file.split('/')[-1]

    if(platform.system() == 'Darwin'):
        location_log = 'logs/kafka_log.log'
        location_log_err = 'logs/crawler_log.log'
    else:
        location_log = r'/home/pi/scrapper/movies/python/logs/kafka_log_'+str(dataDefault)+'-'+file+'.log'
        location_log_err = r'/home/pi/scrapper/movies/python/logs/kafka_err_'+str(dataDefault)+'-'+file+'.log'

    if(tp == 'err'):
        logger_err = logging.getLogger(file)
        hdlr = logging.FileHandler(location_log_err)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger_err.addHandler(hdlr)
        logger_err.setLevel(logging.INFO)
        logger_err.error(msg)
    else:
        with open(location_log,mode='a',newline='') as f:
            f.write(str(msg+"\n"))

############################################################
############# COUCHDB LOGS
############################################################
def couchDBLog(msg,tp):

    if(platform.system() == 'Darwin'):
        location_log = 'logs/couch_log.log'
        location_log_err = 'logs/couch_log.log'
    else:
        location_log = r'/home/pi/scrapper/movies/python/logs/couch_log'+str(dataDefault)+'.log'
        location_log_err = r'/home/pi/scrapper/movies/python/logs/couch_err_'+str(dataDefault)+'.log'

    if(tp == 'err'):
        logger_err = logging.getLogger('couchdb')
        hdlr = logging.FileHandler(location_log_err)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger_err.addHandler(hdlr)
        logger_err.setLevel(logging.INFO)
        logger_err.error(msg)
    else:
        with open(location_log,mode='a',newline='') as f:
            f.write(str(msg+"\n"))

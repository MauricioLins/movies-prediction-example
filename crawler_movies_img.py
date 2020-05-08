#! /usr/bin/env python
# -*- coding: iso-8859-15 -*-
######################################################
# WEB SCRAP MOVIES IMAGES FROM IMDB
# EXAMPLE: python crawler_movies_img.py 'links_2.csv'
######################################################

import requests as rq
from bs4 import BeautifulSoup
import datetime as dt
import sys
import logs
import random
import kafkaProducer

### VARIABLES
######################################################
vfile = sys.argv[1]


def main(filePath):
    print('START: ' + str(dt.datetime.now()))

    with open(filePath) as file:
        for row in file:
            try:
                if (row.split(',')[1] != 'imdbId'):

                    list_elements = []

                    idmovie  = row.split(',')[0]
                    idIMDB   = row.split(',')[1]

                    list_elements.append(['IdMovie',idmovie])
                    list_elements.append(['IdIMDB',idIMDB])

                    urlmovie = 'http://www.imdb.com/title/tt' + idIMDB
                    req = rq.get(urlmovie)
                    bs = BeautifulSoup(req.text, "html.parser")

                    images = bs.find('div', {'class':'poster'}).find('img')
                    image_src = images.get('src')

                    list_elements.append(['image',image_src])

                    dict_elements = dict(list_elements)

                    print(dict_elements)

                    kafkaProducer.kafkaProduc('moviesimages','mimages',dict_elements,vfile)

            except (TypeError, AttributeError, Exception) as e:
                logs.crawlerLog('ERROR: ' + str(vfile), str(e), 'err')
            pass



    print('ENDS: ' + str(dt.datetime.now()))

if __name__ == '__main__': main(vfile)


# FROM jarvis2: /home/pi/scrapper/movies/mdb/1
# nohup python3 /home/pi/scrapper/movies/python/crawler_movies.py /home/pi/scrapper/movies/data/links1.csv &
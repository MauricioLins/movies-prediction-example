#! /usr/bin/env python
# -*- coding: iso-8859-15 -*-
######################################################
# WEB SCRAP MOVIES FROM IMDB
# EXAMPLE: python crawler_movies.py 'links_2.csv'
######################################################

import requests as rq
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from kafka.errors import KafkaError
import datetime as dt
import sys
from crawler import logs
import random

### VARIABLES
######################################################
vfile = sys.argv[1]

### FUNCTIONS
######################################################

# REMOVING SPACES #####
def trimTxt(txt):
	return txt.strip()

# KAFKA PRODUCER #####
def kafkaProduc(topic, vkey, vvalue):
	topic_partitions = [0,1]
	partition = random.choice(topic_partitions)
	try:
		producer = KafkaProducer(bootstrap_servers=['jarvis2:9092'], retries=5)
		future = producer.send(topic, key=str.encode(str(vkey)), value=str.encode(str(vvalue)), partition=partition)
		record_metadata = future.get(timeout=10)
	except KafkaError as nm:
		logs.kafkaLog('ERROR: ' + str(vfile), str(nm), 'err')
		pass
	return


### MAIN
######################################################
def main(filePath):
	print('STARTED: ' + str(dt.datetime.now()))
	with open(filePath) as file:
		for row in file:
			try:
				if (row.split(',')[1] != 'imdbId'):
					idmovie  = row.split(',')[0]
					idIMDB   = row.split(',')[1]
					urlmovie = 'http://www.imdb.com/title/tt'+row.split(',')[1]
					req = rq.get(urlmovie)
					bs = BeautifulSoup(req.text, "html.parser")

					attrlist = []

					content  = []
					# GET WRITERS AND DIRECTORS
					for i in bs.find('div', {'class':'plot_summary_wrapper'}).findAll('div', {'class':'credit_summary_item'}):
						content = i.getText().replace('\n', '').split(':')
						if (content[0] == 'Director'):
							attrlist.append(i.getText().replace('\n', '').split(':'))
						if (content[0] == 'Writers'):
							attrlist.append(i.getText().replace('\n', '').split(':'))

					# GET REVIEWS AND CRITICS
					content  = []
					try:
						for i in bs.find('div', {'class':'plot_summary_wrapper'}).find('div', {'class':'titleReviewBar'}).findAll('div', {'class':'titleReviewBarItem titleReviewbarItemBorder'}):
							#print(i)
							#print(i.getText().replace('\n', '').split())
							content = i.getText().replace('\n', '').split()
							attrlist.append([content[0],content[1]])
							attrlist.append([content[3],content[2].split('|')[1]])
					except (AttributeError, IndexError) as e:
						#print(urlmovie, idmovie, idIMDB, '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
						attrlist.append(['Reviews','0'])
						attrlist.append(['critic','0'])
						pass

					# MOVIE DATAILS AS (INCOME, INVESTIMENT, DURATION, .....)
					content  = []
					for i in bs.find('div', {'id':'titleDetails'}).findAll('div',{'class', 'txt-block'}):
						content = i.getText().replace('\n', '').split(':')
						if (content[0] == 'Country'):
							attrlist.append(i.getText().replace('\n', '').split(':'))
						if (content[0] == 'Language'):
							attrlist.append(i.getText().replace('\n', '').split(':'))
						if (content[0] == 'Budget'):
							attrlist.append(i.getText().replace('\n', '').split(':'))
						if (content[0] == 'Gross'):
							attrlist.append(i.getText().replace('\n', '').split(':'))
						if (content[0] == 'Opening Weekend'):
							attrlist.append(i.getText().replace('\n', '').split(':'))
						if (content[0] == 'Runtime'):
							attrlist.append(i.getText().replace('\n', '').split(':'))

					# ACTORS #####
					try:
						actors = []
						for i in bs.find('table', {'class':'cast_list'}).findAll('span', {'class':'itemprop'}):
							actors.append(i.getText())
						attrlist.append(['actors',actors])
					except (AttributeError, IndexError) as e:
						pass

					# PREPARE AND FORMAT THE DATA AS JSON
					attrlist_final = []
					for x in range(len(attrlist)):

						# DIRECTORS #####
						if (attrlist[x][0] == 'Director'):
							attrlist_final.append([attrlist[x][0],trimTxt(attrlist[x][1])])

						# WRITERS #####
						if (attrlist[x][0] == 'Writers'):
							names = ''
							writersName = attrlist[x][1].split(',')

							for z in range(len(writersName)):
								names = names + trimTxt(writersName[z].split(' (')[0]) + ','
							attrlist_final.append([attrlist[x][0], names[:-1]])

						# BUDGET CURRENCY#####
						if (attrlist[x][0] == 'Budget'):
							attrlist_final.append(['BudgetCurrency', attrlist[x][1].split()[0][:1]])
							attrlist_final.append(['BudgetValue', attrlist[x][1].split()[0][1:]])

						# GROSSS CURRENCY #####
						if (attrlist[x][0] == 'Gross'):
							attrlist_final.append(['GrossCurrency', attrlist[x][1].split()[0][:1]])
							attrlist_final.append(['GrossValue', attrlist[x][1].split()[0][1:]])

						# OPENING WEEKEND CURRENCY#####
						if (attrlist[x][0] == 'Opening Weekend'):
							#print(attrlist[x][1].split()[0])
							attrlist_final.append(['OpeningWeekendCurrency', attrlist[x][1].split()[0][:1]])
							attrlist_final.append(['OpeningWeekendValue', attrlist[x][1].split()[0][1:]])

						# ACTORS #####
						if (attrlist[x][0] == 'actors'):
							#print(attrlist[x][1])
							names = ''
							for z in range(len(attrlist[x][1])):
								names = names + attrlist[x][1][z] + ','
							attrlist_final.append(['Actors',names[:-1]])

						# LANGUAGE #####
						if (attrlist[x][0] == 'Language'):
							attrlist_final.append([attrlist[x][0],trimTxt(attrlist[x][1])])

						# COUNTRY #####
						if (attrlist[x][0] == 'Country'):
							attrlist_final.append([attrlist[x][0],trimTxt(attrlist[x][1])])

						# GET REVIEWS #####
						if (attrlist[x][0] == 'Reviews'):
							attrlist_final.append([attrlist[x][0],trimTxt(attrlist[x][1])])

						# CRITICS #####
						if (attrlist[x][0] == 'critic'):
							attrlist_final.append(['Critic',trimTxt(attrlist[x][1])])

						# MOVIE DURATION #####
						if (attrlist[x][0] == 'Runtime'):
							attrlist_final.append([attrlist[x][0],trimTxt(attrlist[x][1].split()[0])])

					attrlist_final.append(['IdMovie',idmovie])
					attrlist_final.append(['IdIMDB',idIMDB])

					# CRETE THE DICTIONARY WITH EVERYTHING #####
					attrdict = dict(attrlist_final)

					# CHECK IF DICTIONARY CONTAINS ALL DATA NEEDED
					try:
						attrdict['Director']
					except KeyError:
						attrdict['Director'] = ''

					try:
						attrdict['Writers']
					except KeyError:
						attrdict['Writers'] = ''

					try:
						attrdict['Critic']
					except KeyError:
						attrdict['Critic'] = 0

					try:
						attrdict['Runtime']
					except KeyError:
						attrdict['Runtime'] = 0

					try:
						attrdict['Reviews']
					except KeyError:
						attrdict['Reviews'] = 0

					try:
						attrdict['Country']
					except KeyError:
						attrlist_final['Country'] = ''

					try:
						attrdict['Actors']
					except KeyError:
						attrdict['Actors'] = ''

					try:
						attrdict['BudgetCurrency']
					except KeyError:
						attrdict['BudgetCurrency'] = 'NA'

					try:
						attrdict['BudgetValue']
					except KeyError:
						attrdict['BudgetValue'] = 0

					try:
						attrdict['GrossCurrency']
					except KeyError:
						attrdict['GrossCurrency'] = 'NA'

					try:
						attrdict['GrossValue']
					except KeyError:
						attrdict['GrossValue'] = 0

					try:
						attrdict['OpeningWeekendCurrency']
					except KeyError:
						attrdict['OpeningWeekendCurrency'] = 'NA'

					try:
						attrdict['OpeningWeekendValue']
					except KeyError:
						attrdict['OpeningWeekendValue'] = 0

					### KAFKA =========>
					kafkaProduc('moviesdata', 'movies', attrdict)
					print(attrdict)

			except (TypeError, AttributeError, Exception) as e:
				logs.crawlerLog('ERROR: ' + str(vfile), str(e), 'err')
				pass

	print('FINISHED: ' + str(dt.datetime.now()))

if __name__ == '__main__': main(vfile)

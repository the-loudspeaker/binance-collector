#!/usr/bin/env python3
# Importing libraries 
from binance.client import Client
import configparser
from binance.websockets import BinanceSocketManager
import datetime
from datetime import timedelta, timezone
import math
import mysql.connector
import smtplib
import ssl

# Loading keys from config file
config = configparser.ConfigParser()
config.read_file(open('/home/raman/Style/binance-dtb/config.cfg'))
test_api_key = config.get('BINANCE', 'TEST_API_KEY')
test_secret_key = config.get('BINANCE', 'TEST_SECRET_KEY')
actual_api_key = config.get('BINANCE','ACTUAL_API_KEY')
actual_secret_key = config.get('BINANCE','ACTUAL_SECRET_KEY')

client = Client(actual_api_key,actual_secret_key)

prices = client.get_all_tickers() 
## This returns a list of dictionaries which have
## names of markets and their prices.

markets=[]
key, value = 'symbol','price'
for price in prices:
        dictkey=list (price.values())
##        markets.append(dictkey[0])      
## This to be used when we want data for all markets.
## Commenting out for testing.

markets.append("ETHBTC")
markets.append("BTCUSDT")

## setup connection to school database
mydb = mysql.connector.connect(
  host=config.get('BINANCE', 'host'),
  user=config.get('BINANCE', 'user'),
  password=config.get('BINANCE', 'password'),
  database="school"
)

mycursor = mydb.cursor()

smtp_server="disroot.org"
port = 587
sender_email= "theloudspeaker@disroot.org"
receiver_email = "ramansarda2000@gmail.com"  # Enter receiver address
###password = input("Type your password and press enter: ")


qp=[]
for market in markets:
    executecmd="SELECT COUNT(*) from "+market
    mycursor.execute(executecmd)
    lastrownum=mycursor.fetchone()[0]
    print(lastrownum)
    if ((lastrownum % 60)==0 and lastrownum !=0):
        qp.append(round(float(lastrownum/60), 3))

print(qp)
arraytosend=[]

for i in range(len(markets)):
    arraytosend.append([markets[i], qp[i]])


message = """\
Subject: Quality check report

PFA a file containing quality coefficient for each market."""

## send mail
"""
context=ssl.create_default_context()
with smtplib.SMTP(smtp_server, port) as server:
    server.ehlo()  # Can be omitted
    server.starttls(context=context)
    server.ehlo()  # Can be omitted
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, message)
"""
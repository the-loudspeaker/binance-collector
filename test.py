#!/usr/bin/env python3
# Importing libraries 
from binance.client import Client
import configparser
from binance.websockets import BinanceSocketManager
import datetime
from datetime import timedelta, timezone
import math
import mysql.connector

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


## note date and time when script is run.
startdate=datetime.datetime.now(tz=timezone.utc)
startdate=startdate-datetime.timedelta(minutes=2)
endate=startdate+datetime.timedelta(minutes=1)
startdate=datetime.datetime.strftime(startdate,"%H:%M:%S %d-%m-%Y")
endate=datetime.datetime.strftime(endate,"%H:%M:%S %d-%m-%Y")

## setup connection to school database
mydb = mysql.connector.connect(
  host=config.get('BINANCE', 'host'),
  user=config.get('BINANCE', 'user'),
  password=config.get('BINANCE', 'password'),
  database="school"
)

mycursor = mydb.cursor()

######################################################################################################
## BOOTSTRAP

for market in markets:
    ## create 1m table
    print("Creating a table for ", market,"in mysql database named school")
    createtablecommand="CREATE TABLE IF NOT EXISTS "+market+" (opentime DATETIME, closetime DATETIME, open FLOAT(20,3), close FLOAT(20,3), high FLOAT(20,3), low FLOAT(20,3), volume FLOAT(20,3))"
    mycursor.execute(createtablecommand)

    ##create 5m table
    print("Creating 5m table for "+ market)
    executecmd="CREATE TABLE IF NOT EXISTS "+market+"_5m (opentime DATETIME, closetime DATETIME, open FLOAT(20,3), close FLOAT(20,3), high FLOAT(20,3), low FLOAT(20,3), volume FLOAT(20,3))"
    mycursor.execute(executecmd)

    ##create 15m table
    print("Creating 15m table for "+ market)
    executecmd="CREATE TABLE IF NOT EXISTS "+market+"_15m (opentime DATETIME, closetime DATETIME, open FLOAT(20,3), close FLOAT(20,3), high FLOAT(20,3), low FLOAT(20,3), volume FLOAT(20,3))"
    mycursor.execute(executecmd)

    ##create 30m table
    print("Creating 30m table for "+ market)
    executecmd="CREATE TABLE IF NOT EXISTS "+market+"_30m (opentime DATETIME, closetime DATETIME, open FLOAT(20,3), close FLOAT(20,3), high FLOAT(20,3), low FLOAT(20,3), volume FLOAT(20,3))"
    mycursor.execute(executecmd)

    ##create 1m table
    print("Creating 1h table for "+ market)
    executecmd="CREATE TABLE IF NOT EXISTS "+market+"_1h (opentime DATETIME, closetime DATETIME, open FLOAT(20,3), close FLOAT(20,3), high FLOAT(20,3), low FLOAT(20,3), volume FLOAT(20,3))"
    mycursor.execute(executecmd)


######################################################################################################


## Collect 1m data and store into sql tables.

for market in markets:
    print("Getting data for ",market)
    klines = client.get_historical_klines(market, Client.KLINE_INTERVAL_1MINUTE, startdate, endate)
    for kline in klines:
        opentime=datetime.datetime.utcfromtimestamp(kline[0]/1000).strftime('%Y-%m-%d %H:%M:%S')
        closetime=datetime.datetime.utcfromtimestamp(kline[6]/1000).strftime('%Y-%m-%d %H:%M:%S')
        openval=round(float(kline[1]),3)
        closeval=round(float(kline[4]),3)
        high=round(float(kline[2]),3)
        low=round(float(kline[3]),3)
        volume=round(float(kline[5]),3)
        sql="INSERT INTO "+market+" (opentime, closetime, open,close, high, low, volume) VALUES (%s, %s, %s, %s,%s, %s, %s)"
        val=(opentime, closetime, openval,closeval, high, low, volume)
        mycursor.execute(sql, val)
        mydb.commit()
    
    ## Convert 1m to 5m.
    executecmd="SELECT COUNT(*) from "+market
    mycursor.execute(executecmd)
    lastrownum=mycursor.fetchone()[0]
    print(lastrownum)
    if ((lastrownum % 5)==0 and lastrownum !=0):
        executecmd="SELECT * FROM (SELECT * FROM "+market+" ORDER by opentime DESC LIMIT 5) sub ORDER BY opentime ASC"
        mycursor.execute(executecmd)
        newrowlist=(mycursor.fetchall())
        newopentime=newrowlist[0][0]
        newclosetime=newrowlist[4][1]
        newopenval=newrowlist[0][2]
        newcloseval=newrowlist[4][3]
        newhighval=max([row[4]for row in newrowlist])
        newlowval=min([row[5]for row in newrowlist])
        newvolumeval=sum([row[6]for row in newrowlist])
        print("Writing data to new table ", market)
        sql="INSERT INTO "+market+"_5m (opentime, closetime, open,close, high, low, volume) VALUES (%s, %s, %s, %s,%s, %s, %s)"
        val=(newopentime, newclosetime, newopenval, newcloseval, newhighval, newlowval, newvolumeval)
        mycursor.execute(sql, val)
        mydb.commit()

    ## convert 1m to 15m
    executecmd="SELECT COUNT(*) from "+market
    mycursor.execute(executecmd)
    lastrownum=mycursor.fetchone()[0]
    print(lastrownum)
    if ((lastrownum % 15)==0 and lastrownum !=0):
        executecmd="SELECT * FROM (SELECT * FROM "+market+" ORDER by opentime DESC LIMIT 15) sub ORDER BY opentime ASC"
        mycursor.execute(executecmd)
        newrowlist=(mycursor.fetchall())
        newopentime=newrowlist[0][0]
        newclosetime=newrowlist[4][1]
        newopenval=newrowlist[0][2]
        newcloseval=newrowlist[4][3]
        newhighval=max([row[4]for row in newrowlist])
        newlowval=min([row[5]for row in newrowlist])
        newvolumeval=sum([row[6]for row in newrowlist])
        print("Writing 15m data to new table ", market)
        sql="INSERT INTO "+market+"_15m (opentime, closetime, open,close, high, low, volume) VALUES (%s, %s, %s, %s,%s, %s, %s)"
        val=(newopentime, newclosetime, newopenval, newcloseval, newhighval, newlowval, newvolumeval)
        mycursor.execute(sql, val)
        mydb.commit()

    ## Convert 1m to 30m
    executecmd="SELECT COUNT(*) from "+market
    mycursor.execute(executecmd)
    lastrownum=mycursor.fetchone()[0]
    print(lastrownum)
    if ((lastrownum % 30)==0 and lastrownum !=0):
        executecmd="SELECT * FROM (SELECT * FROM "+market+" ORDER by opentime DESC LIMIT 30) sub ORDER BY opentime ASC"
        mycursor.execute(executecmd)
        newrowlist=(mycursor.fetchall())
        newopentime=newrowlist[0][0]
        newclosetime=newrowlist[4][1]
        newopenval=newrowlist[0][2]
        newcloseval=newrowlist[4][3]
        newhighval=max([row[4]for row in newrowlist])
        newlowval=min([row[5]for row in newrowlist])
        newvolumeval=sum([row[6]for row in newrowlist])
        print("Writing 30m data to new table ", market)
        sql="INSERT INTO "+market+"_30m (opentime, closetime, open,close, high, low, volume) VALUES (%s, %s, %s, %s,%s, %s, %s)"
        val=(newopentime, newclosetime, newopenval, newcloseval, newhighval, newlowval, newvolumeval)
        mycursor.execute(sql, val)
        mydb.commit()

    ## convert 1m to 1h
    executecmd="SELECT COUNT(*) from "+market
    mycursor.execute(executecmd)
    lastrownum=mycursor.fetchone()[0]
    print(lastrownum)
    if ((lastrownum % 60)==0 and lastrownum !=0):
        executecmd="SELECT * FROM (SELECT * FROM "+market+" ORDER by opentime DESC LIMIT 60) sub ORDER BY opentime ASC"
        mycursor.execute(executecmd)
        newrowlist=(mycursor.fetchall())
        newopentime=newrowlist[0][0]
        newclosetime=newrowlist[4][1]
        newopenval=newrowlist[0][2]
        newcloseval=newrowlist[4][3]
        newhighval=max([row[4]for row in newrowlist])
        newlowval=min([row[5]for row in newrowlist])
        newvolumeval=sum([row[6]for row in newrowlist])
        print("Writing 1h data to new table ", market)
        sql="INSERT INTO "+market+"_1h (opentime, closetime, open,close, high, low, volume) VALUES (%s, %s, %s, %s,%s, %s, %s)"
        val=(newopentime, newclosetime, newopenval, newcloseval, newhighval, newlowval, newvolumeval)
        mycursor.execute(sql, val)
        mydb.commit()
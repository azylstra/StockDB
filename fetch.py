# Fetch stock data from Yahoo Finance
# If databases exist, then append to them
# Otherwise, start database from scratch.
#
# Author: Alex Zylstra
# Date: 2014/04/16

import os
import sys
import io
import pickle
import urllib.request, urllib.parse, urllib.error
import csv
import time
import datetime

from DB import DB, FILE


# Data that we want:
# see http://www.jarloo.com/yahoo_finance/
# format is [ symbol , description , type ]
# type = f (float)
#        s (string)
#        d (date)
Data_To_Fetch = [   [ 'o' , 'Open', 'f'] ,
                    [ 'g' , 'Low', 'f'] ,
                    [ 'h' , 'High', 'f'] ,
                    [ 't8' , 'Target', 'f'] ,
                    [ 'k' , '52 week high', 'f'] ,
                    [ 'j' , '52 week low', 'f'] ,
                    [ 'v' , 'Volume', 'f'] ,
                    [ 'e' , 'EPS', 'f'] ,
                    [ 'e7' , 'EPS Estimate (current year)', 'f'] ,
                    [ 'e8' , 'EPS Estimate (next year)', 'f'] ,
                    [ 'b4' , 'Book Value', 'f'] ,
                    [ 'j4' , 'EBITDA', 'f'] ,
                    [ 'p5' , 'Price/Sales', 'f'] ,
                    [ 'p6' , 'Price/Book', 'f'] ,
                    [ 'r' , 'P/E', 'f'] ,
                    [ 'r5' , 'PEG', 'f'] ,
                    [ 'r6' , 'P/EPS Estimate (current year)', 'f'] ,
                    [ 'r7' , 'P/EPS Estimate (next year)', 'f'] ,
                    [ 's7' , 'Short Ratio', 'f'] ,
                    [ 'y' , 'Dividend Yield', 'f'] ,
                    [ 'd' , 'Dividend', 'f'],
                    [ 'r1' , 'Dividend Pay Date', 's'],
                    [ 'q' , 'Ex-Dividend Date', 's'],
                    [ 'm3' , '50-day moving average', 'f'] ,
                    [ 'm4' , '200-day moving average', 'f'] ,
                    [ 'j1' , 'Market Cap', 'f'] ]
URL_Base = 'http://finance.yahoo.com/d/quotes.csv?s='

def fetch_yahoo(sym):
    """Get data from Yahoo for a given stock symbol (ticker). Returns a `dict`."""
    URL = URL_Base + sym
    URL += '&f='
    for i in Data_To_Fetch:
        URL += i[0]

    # read in data:
    sock = urllib.request.urlopen(URL)
    raw = sock.read()
    sock.close()
    raw = raw.decode('utf-8')

    # convert to array of strings:
    raw2 = []
    reader = csv.reader(io.StringIO(raw), delimiter=',')
    for i in reader:
        raw2.append(i)
    raw2 = raw2[0]

    #parse data to array:
    data = []
    for i in range(len(raw2)):
        if Data_To_Fetch[i][2] == 'f' and raw2[i] != 'N/A':
            temp = raw2[i].replace( 'M','e6' )
            temp = temp.replace( 'B','e9' )
            temp = temp.replace( 'T','e12' )
            temp = temp.replace( 'K','e3' )
            data.append(float(temp))
        else:
            data.append(raw2[i])

    # add request info to the data.
    for i in range(len(data)):
        data[i] = Data_To_Fetch[i] + [data[i]]
    
    # today's date:
    dt = datetime.date.today()
    date = str(dt.year) + '-' + str(dt.month) + '-' + str(dt.day)

    # data is returned as a dict
    # add to the data:
    ret = {'Date': date}

    # Add the rest of the data:
    for row in data:
        ret[row[1]] = row[3]

    return ret

def add_data_db(sym, db):
    """Fetch data for symbol `sym` and add it to the database `db`."""
    data = fetch_yahoo(sym)

    # Build data:
    tbd = (data['Date'],
            sym,
            data['Open'],
            data['Low'],
            data['High'],
            data['Target'],
            data['52 week high'],
            data['52 week low'],
            data['Volume'],
            data['EPS'],
            data['EPS Estimate (current year)'],
            data['EPS Estimate (next year)'],
            data['Book Value'],
            data['EBITDA'],
            data['Price/Sales'],
            data['Price/Book'],
            data['P/E'],
            data['PEG'],
            data['P/EPS Estimate (current year)'],
            data['P/EPS Estimate (next year)'],
            data['Short Ratio'],
            data['Dividend Yield'],
            data['Dividend'],
            data['Dividend Pay Date'],
            data['Ex-Dividend Date'],
            data['50-day moving average'],
            data['200-day moving average'],
            data['Market Cap'],)

    db.sql_query('INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)' % db.TABLE, tbd)


def add_all_to_db():
    """Get a list of all symbols in the database, and fetch new data."""
    # Connect to the database
    db = DB(FILE)

    # Get symbols from the ticker table
    query = db.sql_query('SELECT Distinct symbol from %s;' % db.TABLE_SYM)

    symbols = []
    for row in query:
        symbols.append(row[0])

    for sym in symbols:
        # today's date:
        dt = datetime.date.today()
        date = str(dt.year) + '-' + str(dt.month) + '-' + str(dt.day)
        # Check to see if the data is already there
        query = db.sql_query('SELECT date from %s WHERE date=? AND symbol=?;' % db.TABLE, (date,sym,))

        if len(query.fetchall()) == 0:
            #print 'does not exist!'
            add_data_db(sym, db)

def load_old_csv(fname, sym=None):
    """Import data from an old CSV file, and add it to the database."""
    # If no symbol is supplied, get it from the filename:
    if sym is None:
        temp = os.path.split(fname)[-1]
        temp = temp.split('.')[0]
        sym = temp

    # Connect to the database
    db = DB(FILE)

    with open(fname, 'rU') as csvfile:
        # Read in the data and parse so that each element in raw
        # corresponds to one dataset
        raw = []
        temp = []
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in reader:
            if row[1] == 'Date':
                if len(temp) > 0:
                    raw.append(temp)
                temp = [row]
            else:
                temp.append(row)

        # Now we need to convert to data that can be inserted into the database
        all_data = []
        for row in raw:
            # Convert into a dict, which is convenient for the next step
            date = row[0][3].replace('[','').replace(']','').replace(', ','-')
            raw2 = {'Date': date}

            for x in row[1:]:
                # ugly but needed to fix a previous mistake:
                if x[1] == 'Divident Pay Date':
                    raw2['Dividend Pay Date'] = x[3]
                else:
                    raw2[x[1]] = x[3]

            all_data.append(raw2)

        # Now put it into the database:
        for data in all_data:
            # Check to see if the data is already there
            query = db.sql_query('SELECT date from %s WHERE date=? AND symbol=?;' % db.TABLE, (date,sym,))

            if len(query.fetchall()) == 0:
                # Build data:
                tbd = (data['Date'],
                        sym,
                        data['Open'],
                        data['Low'],
                        data['High'],
                        data['Target'],
                        data['52 week high'],
                        data['52 week low'],
                        data['Volume'],
                        data['EPS'],
                        data['EPS Estimate (current year)'],
                        data['EPS Estimate (next year)'],
                        data['Book Value'],
                        data['EBITDA'],
                        data['Price/Sales'],
                        data['Price/Book'],
                        data['P/E'],
                        data['PEG'],
                        data['P/EPS Estimate (current year)'],
                        data['P/EPS Estimate (next year)'],
                        data['Short Ratio'],
                        data['Dividend Yield'],
                        data['Dividend'],
                        data['Dividend Pay Date'],
                        data['Ex-Dividend Date'],
                        data['50-day moving average'],
                        data['200-day moving average'],
                        data['Market Cap'],)

                db.sql_query('INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)' % db.TABLE, tbd)


# Some scripts for interacting with the stock database
# 
# Author: Alex Zylstra
# Date: 2014/04/13

import os
import csv
from DB import DB, FILE

def load_exchange(fname, delim='\t', exchange='', clear=False, header=True):
    """Load exchange data (tickers and info about them) from a CSV file."""
    with open(fname, 'rU') as csvfile:
        # Open a connection to the DB:
        db = DB(FILE)

        header_text = ''
        data = []

        # Read in everything from the CSV file:
        reader = csv.reader(csvfile, delimiter=delim, quotechar='|')
        for row in reader:
            if header and header_text=='':
                header_text = row
            else:
                data.append(row)

        # If requested, delete everything from the current database
        if clear:
            db.sql_query('''DELETE from %s WHERE exchange=?;''' % db.TABLE_SYM, (exchange,))


        # Iterate over data and insert into the database
        for row in data:
            try:
                # fetch data
                sym = row[0]
                name = row[1]
                ipo = row[5]
                if ipo == 'n/a':
                    ipo = ''
                sector = row[6]
                industry = row[7]

                # insert into db
                db.sql_query('INSERT INTO %s VALUES (?,?,?,?,?,?)' % db.TABLE_SYM, 
                    (sym,name,exchange,ipo,sector,industry,))
            except:
                print('trouble with row: ', row)

def load_all_exchange():
    """Shortcut script to load all exchange data from files."""
    load_exchange('data_exchange/AMEX.tsv', exchange='AMEX', delim='\t', clear=True, header=True)
    load_exchange('data_exchange/NASDAQ.tsv', exchange='NASDAQ', delim='\t', clear=True, header=True)
    load_exchange('data_exchange/NYSE.tsv', exchange='NYSE', delim='\t', clear=True, header=True)
    
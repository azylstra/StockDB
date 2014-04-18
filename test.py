#!/usr/bin/python3

from DB import DB, FILE
from scripts import *
from fetch import *

db = DB(FILE)

#add_data_db('AAPL', db)
#load_all_exchange()
#add_all_to_db()

load_old_csv('/Users/alex/Desktop/AAPL.csv')
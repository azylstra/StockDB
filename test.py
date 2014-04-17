#!/usr/bin/python

from DB import DB, FILE
from scripts import *
from fetch import *

db = DB(FILE)

#add_data_db('AAPL', db)

add_all_to_db()
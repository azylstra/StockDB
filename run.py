#!/usr/bin/python3
# Run the regular daily import of data, with error loggin

from DB import DB, FILE
from scripts import *
from fetch import *
import datetime

import logging
logging.basicConfig(filename='StockDB.log',level=logging.INFO)
import smtplib

def run():
    """Run the daily data import."""
    errors = add_all_to_db()

    for err in errors:
        logging.error("Error: {0}".format(err))

    # Attempt to email:
    try:
        dt = datetime.date.today()
        date = str(dt.year) + '-' + str(dt.month) + '-' + str(dt.day)
        fromaddr = 'root@db.azylstra.net'
        toaddrs  = 'alex@azylstra.net'.split()
        # Construct the message
        subject = "StockDB report"
        body = 'Date: ' + date + '\n'
        body += 'Number of errors: ' + str(len(errors)) + '\n\n'
        for err in errors:
            body += "Error: {0}".format(err) + '\n'
        msg = 'Subject: %s\n\n%s' % (subject, body)

        server = smtplib.SMTP('localhost')
        server.sendmail(fromaddr, toaddrs, msg)
        server.quit()
    except Exception as err:
        logging.error("Error: {0}".format(err))
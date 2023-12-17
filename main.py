import database
from globals import *
import sys
import logging
import argparse
from datetime import datetime, date
from dateutil.relativedelta import relativedelta, MO

import scraper
import histScraper

FIRST_ARGUMENT = 'operation'
SECOND_ARGUMENT = 'date'
OPTION_DAILY = 'daily'
OPTION_WEEKLY = 'weekly'
OPTION_WEEKLY_FAST = 'weekly_fast'
OPTION_NOW = 'now'
OPTION_NOW_USA = 'usa'
OPTION_HIST = 'hist'
OPTION_HIST_ALL = 'hist_all'
OPTION_UPDATE_SECTORS = 'update_sectors'
OPTION_UPDATE_SECTORS_INV = 'update_sectors_inv'
OPTION_UPDATE_STOCKS = 'update_stocks'
OPTION_UPDATE_CURRENCIES = 'update_currencies'
OPTION_TEST = 'test'


# Instantiate the parser
parser = argparse.ArgumentParser(description='Stock quotes scrapper from Investing.com')
# Optional positional argument
parser.add_argument(FIRST_ARGUMENT, type=str, nargs='?', default=OPTION_NOW,
                    help='Main option: ')
# Second optional argument with a date
parser.add_argument(SECOND_ARGUMENT, type=str, nargs='?',
                    help='Optional date: ')

argsNamespace = parser.parse_args()
args = vars(argsNamespace)

logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s %(name)s - %(message)s')


if args[FIRST_ARGUMENT] == OPTION_NOW:
    scraper.scrapeCurrentPricesIndex(ZONE_EU)
elif args[FIRST_ARGUMENT] == OPTION_NOW_USA:
    scraper.scrapeCurrentPricesIndex(ZONE_USA)
elif args[FIRST_ARGUMENT] == OPTION_HIST:
    try:
        date_obj = datetime.strptime(args[SECOND_ARGUMENT], '%d/%m/%Y')
    except ValueError:
        print('"hist" option needs a date in format dd/mm/yyyy')
        sys.exit(1)
    histScraper.scrapeHistPrices(DB_PATH, date_obj,  "")
elif args[FIRST_ARGUMENT] == OPTION_HIST_ALL:
    try:
        date_obj = datetime.strptime(args[SECOND_ARGUMENT], '%d/%m/%Y')
    except ValueError:
        print('"hist_all" option needs a date in format dd/mm/yyyy')
        sys.exit(1)
    histScraper.scrapeHistPrices(DB_PATH, date_obj, "", True)
elif args[FIRST_ARGUMENT] == OPTION_UPDATE_SECTORS:
    scraper.updateStocksSectors(DB_PATH)
elif args[FIRST_ARGUMENT] == OPTION_UPDATE_SECTORS_INV:
    scraper.updateSectors(DB_PATH, "WHERE id > 1072")
elif args[FIRST_ARGUMENT] == OPTION_UPDATE_STOCKS:
    scraper.updateNewStocks(DB_PATH, "WHERE id == 1177")
elif args[FIRST_ARGUMENT] == OPTION_UPDATE_CURRENCIES:
    scraper.updateCurrencies(DB_PATH)
elif args[FIRST_ARGUMENT] == OPTION_DAILY:  # Has to be executed before midnight
    database.backupDatabase()
    scraper.scrapeCurrentPricesIndex(ZONE_EU)
    scraper.scrapeCurrentPricesIndex(ZONE_USA)
    scraper.scrapeCurrentPricesMainSelect("""SELECT S.*, H.date 
                        FROM STOCKS S LEFT JOIN HIST H ON S.id = H.stock_id AND H.date = CURRENT_DATE
                        WHERE H.date is NULL""")  # Select all stocks with no hist data for today
    scraper.updateAllStocksLastCloseAndPorcDiv()
    scraper.calculateCoefs()
elif args[FIRST_ARGUMENT] == OPTION_WEEKLY or args[FIRST_ARGUMENT] == OPTION_WEEKLY_FAST:  # Executed on weekends
    logging.warning('Weekly in')
    database.backupDatabase()

    date_obj = datetime.today() + relativedelta(days=-22)
    logging.warning(f'date {date_obj}')

    if args[FIRST_ARGUMENT] == OPTION_WEEKLY:
        scraper.updateCurrencies(DB_PATH)
        logging.warning('UpdateCurrencies out')
        scraper.updateAllDataStocks(DB_PATH, '')
        logging.warning('updateAllDataStocks out')
    histScraper.scrapeHistPrices(DB_PATH, date_obj, "", True)
    logging.warning('scraoeHistPrices out')
    scraper.updateAllStocksLastCloseAndPorcDiv()
    logging.warning('updateAllStocksLastClose... out')
    scraper.calculateCoefs()
    logging.warning('Weekly out')
elif args[FIRST_ARGUMENT] == OPTION_TEST:
    date_obj = datetime.today() + relativedelta(days=-22)
    histScraper.testScrapeHistPrices(DB_PATH, date_obj, "", True)

else:
    print(args[FIRST_ARGUMENT] + ' is not a recognized option, try "daily", "hist <date>"')

sys.exit(0)

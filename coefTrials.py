import database
import sqlite3 as sl
from datetime import date, timedelta
import sqliteUtil
import util

CALCULATED_HIST = "temp_hist"


def coefDeleteTables(dbCon: sl.Connection):
    pass


def doOneCoef(dbCon: sl.Connection, baseDate: date, numDaysTot):
    database.fillCoefTables(dbCon, baseDate, numDaysTot)




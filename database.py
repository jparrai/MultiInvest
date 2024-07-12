from globals import *
import sqliteUtil
import sqlite3 as sl
from datetime import datetime, timedelta, date
from shutil import copyfile
from pathlib import PureWindowsPath, Path
import os
import numpy as np


# Makes a copy of the current database file to a sub-directory called 'backup', prefixing the date
def backupDatabase():
    dbDir = PureWindowsPath(DB_PATH).parent  # directory of db File (PureWindowsPath object)
    dbName = PureWindowsPath(DB_PATH).name # file name of db (string)
    backupDir = dbDir / "backups" # subdirectory for backups (PureWindowsPath object)
    # Create directory if it does not exist
    Path(backupDir).mkdir(parents=True, exist_ok=True)
    backupName = str(date.today().isoformat()) + "-" + dbName
    backupPath = backupDir / backupName
    copyfile(DB_PATH, str(backupPath))
    print(str(backupPath))


# Check de database and returns true if it's ok, false if it's not
def check():
    dbCon = sl.connect(DB_PATH)
    cursor = dbCon.cursor()

    isOk = cursor.execute(f"""
                   PRAGMA integrity_check
                       """).fetchone()[0]
    dbCon.close()

    return isOk == "ok"


# Caclulate the coef
def calculateCoef(dbCon):
    COEF_STOCK = 1
    COEF_MARKET = 1
    COEF_ZONE = 1
    COEF_INDUSTRY = 1
    COEF_SECTOR = 1

    dbCon.execute(f"""
                      UPDATE temp_stocks
                      SET coef = ( 
                        delta * {COEF_STOCK} + 
                        delta_market * {COEF_MARKET} +
                        delta_zone * {COEF_ZONE} +
                        delta_zone_industry * {COEF_INDUSTRY} +
                        delta_zone_sector * {COEF_SECTOR} 
                      ) / 5
                  """)


def _coefUpVol(change, close):
    try:
        porcChange = change * 100 / (close - change) if (close - change) != 0 else 0
        return 1 / (1 + np.exp(-porcChange))

    except Exception as e:
        print(e, close, change)


# Fill the coefs tables "temp_*" from the dataBase date (or first one with hist data)
# for as many days back as numDaysTot say
def fillCoefTables(dbCon, dateBase, numDaysTot=1):
    # We calculate the geometric average to downplay extreme values
    # g_mean is a function that we add to sqlite for that purpose
    dbCon.create_aggregate("g_avg", 1, sqliteUtil.GeometricAvg)
    # corr will calculate the correlation between two columns
    dbCon.create_aggregate("corr", 2, sqliteUtil.PearsonsCorr)
    # upVolCoef gets the upside volume from the volume
    dbCon.create_function("upVolCoef", 2, _coefUpVol)
    # Fill the temporary table with data from the historic between 90 days (plus the days we want)
    # before our base date, and seven weeks after
    fillCalculatedHist(dbCon, dateBase - timedelta(days=(SESSIONS_AVG + numDaysTot) * 1.5),
                       dateBase + timedelta(weeks=7, days=-1))
    cursor = dbCon.cursor()
    day = timedelta(days=1)

    fillCoefTable(dbCon, 'coef_zone', 'zone')
    fillCoefTable(dbCon, 'coef_market', 'market')
    fillCoefTable(dbCon, 'coef_zone_sector', 'zone, sector')
    fillCoefTable(dbCon, 'coef_zone_industry', 'zone, industry')
    fillCoefTable(dbCon, 'coef_stock', 'stock_id')

    dbCon.commit()


# NOT adapted to dates format
def futureCheck(dbCon, dateBase):
    updateColClose(dbCon, 'close_plus3',
                   dateBase + timedelta(weeks=3), dateBase + timedelta(weeks=3, days=5))
    updateColClose(dbCon, 'close_plus6',
                   dateBase + timedelta(weeks=6), dateBase + timedelta(weeks=6, days=5))

    updateGain(dbCon)
    dbCon.execute("""INSERT INTO temp_correlations (test_id, correlation)
                         VALUES ('3-6 weeks avg 1,1,1,1,1 up linear 0.1 - 0.9', 
                            ( SELECT corr(coef-1,gain)
                                FROM temp_stocks
                                WHERE coef IS NOT NULL
                            )
                        )
                        """)
    dbCon.commit()


# Calculate the geometric average volume in euros for differente groupings.
# You need to have defined the upVolCoef for sqlite function,
# and the g_avg function if we are going to use it, so we define them anyway
# For each group (zone, market, industry...) we first get the sum of volumes
# for each date in the range, and then the average of those (just one value for group)
# and the ratio between the averages.
def execAvgVolQuery(dbCon, table, grouping, dateIni, dateEnd, clear=False):
    cursor = dbCon.cursor()
    if clear:
        cursor.execute('DELETE FROM ' + table)
    dbCon.create_function("upVolCoef", 2, _coefUpVol)
    dbCon.create_aggregate("g_avg", 1, sqliteUtil.GeometricAvg)
    dateIniStr = dateIni.strftime('%Y-%m-%d')
    dateEndStr = dateEnd.strftime('%Y-%m-%d')
    cursor.execute(f"""
                    INSERT INTO {table} ({grouping}, eurVol, eurVolUp, ratio, date)
                        SELECT  {grouping}, 
                                round(avg(sumVol),0) eurVol, 
                                round(avg(sumVolUp),0) eurVolUp,
                                (avg(sumVolUp) / avg(sumVol) ) ratio,
                                '{dateEndStr}'
                        FROM 
                            ( SELECT {grouping}, sum(volume*changeEur) sumVol, 
                                                 sum(volume*upVolCoef(change, close)*changeEur) sumVolUp 
                                FROM temp_hist
                                WHERE date >= '{dateIniStr}' AND date <= '{dateEndStr}' 
                                GROUP BY {grouping}, date 
                            )
                        GROUP BY {grouping}
                    """)
    dbCon.commit()


# Calculate the geometric average volume in euros for differente groupings.
# You need to have defined the upVolCoef for sqlite function,
# and the g_avg function if we are going to use it, so we define them anyway
# For each group (zone, market, industry...) we first get the sum of volumes
# for each date in the range, and then the average of those (just one value for group)
# and the ratio between the averages.
def fillCoefTable(dbCon, table, grouping):
    cursor = dbCon.cursor()
    dbCon.create_function("upVolCoef", 2, _coefUpVol)
    dbCon.create_aggregate("g_avg", 1, sqliteUtil.GeometricAvg)
    cursor.execute('DELETE FROM ' + table)

    cursor.execute(f"""
                    INSERT INTO {table} ({grouping}, date, eurVol, eurVolUp)
                        SELECT {grouping},
                                date, 
                                round(sum(eurVol),0) eurVol, 
                                round(sum(eurVolUp),0) eurVolUp
                                FROM temp_hist 
                                GROUP BY {grouping}, date 
                    """)
    # Converts a expression of the form: "col1, col2" to "T.col1 = table.col1 AND T.col2 = table.col2"
    # with "table" replaced by the table name parameter
    grouping_condition = ' AND '.join('T.'+item.strip()+' = '+table+'.'+item.strip() for item in grouping.split(','))
    cursor.execute(f"""
                        UPDATE {table} SET ratio = round(CAST(eurVolUp AS REAL)/ eurVol,3),
                            eurVolAvg = ( SELECT ROUND(g_avg(eurVol),0)
                                            FROM {table} T
                                            WHERE {grouping_condition}
                                                  AND T.date < {table}.date
                                            LIMIT {SESSIONS_AVG}
                                            ),
                            eurVolUpAvg = ( SELECT ROUND(g_avg(eurVolUp),0)
                                            FROM {table} T
                                            WHERE {grouping_condition}
                                                  AND T.date < {table}.date
                                            LIMIT {SESSIONS_AVG}
                                            )   
                        """)
    cursor.execute(f"""
                            UPDATE {table} SET ratioAvg = round(CAST(eurVol AS REAL)/ eurVolAvg,3),
                                               ratioUpAvg = round(CAST(eurVolUp AS REAL)/ eurVolUpAvg,3)
                            """)
    cursor.execute(f"""
                                UPDATE {table} SET ratioComb = ratio * iif(ratioAvg > 1.5, 1.2, 1)
                                """)
    dbCon.commit()


# Fills the base table with the historic rows we want, completed with all needed related data
# The function upVolCoef needs to be defined
def fillCalculatedHist(dbCon: sl.Connection, dateIni: datetime.date, dateEnd: datetime.date):
    cursor = dbCon.cursor()
    cursor.execute('DELETE FROM temp_hist')
    dbCon.create_function("upVolCoef", 2, _coefUpVol)
    cursor.execute(f"""
                    INSERT INTO temp_hist (stock_id, date, change, close, 
                                            porcChange, zone, sector, industry, 
                                            eurVol, eurVolUp, 
                                            ratio, changeEur, market, volume)
                        SELECT  stock_id, date, change, close , 
                                change*100 / (close - change), zone, sector, industry,
                                volume*changeEur eurVol, volume*upVolCoef(change, close)*changeEur eurVolUp,   
                                upVolCoef(change, close) ratio, changeEur, market, volume
                        FROM HIST H
                                LEFT JOIN STOCKS S ON S.id = H.stock_id
                                LEFT JOIN CURRENCIES C ON S.currency = C.id
                        WHERE H.date >= '{dateIni.strftime('%Y-%m-%d')}' AND H.date <= '{dateEnd.strftime('%Y-%m-%d')}'
                    """)
    dbCon.commit()


def getLastXSessionsDate(cursor, dateBase, numOfSessions):
    # First find the start date for the last "numOfSessions" sessions
    dateIni = cursor.execute(f"""
                SELECT MIN(date)
                FROM (SELECT DISTINCT date
                      FROM HIST
                      WHERE date <= '{dateBase.strftime('%Y-%m-%d')}'
                      ORDER BY Date DESC
                      LIMIT {numOfSessions} )
                    """).fetchone()[0]
    return datetime.strptime(dateIni, '%Y-%m-%d') if dateIni is not None else None


# Update the close value for different time frames in the temp_stocks table
def updateColClose(dbCon, column, dateIni, dateEnd):
    dbCon.execute(f"""
                    UPDATE temp_stocks
                        SET {column} = (   SELECT avg(close)
                                        FROM temp_hist
                                        WHERE stock_id = temp_stocks.stock_id 
                                                AND date >= '{dateIni.strftime('%Y-%m-%d')}' 
                                                AND date <= '{dateEnd.strftime('%Y-%m-%d')}'
                                        GROUP BY stock_id
                                    )
                    WHERE date = '{dateEnd.strftime('%Y-%m-%d')}'
                """)


# Updates the delta (ratio between the upVol in the stock and in the given table) in the temp_stocks table
def updateDeltas(dbCon):
    dbCon.execute("""
                      UPDATE temp_stocks
                      SET delta = ratio 
                          - 
                          ( SELECT ratio
                          FROM temp_stocks_avg
                          WHERE temp_stocks_avg.stock_id = temp_stocks.stock_id
                            AND temp_stocks_avg.date = temp_stocks.date
                          )
                  """)
    dbCon.execute("""
                    UPDATE temp_stocks
                    SET delta_zone = ratio 
                        - 
                        ( SELECT ratio
                        FROM temp_zone
                        WHERE zone = (SELECT zone FROM STOCKS WHERE id = temp_stocks.stock_id)
                          AND temp_zone.date = temp_stocks.date
                        )
                """)
    dbCon.execute("""
                    UPDATE temp_stocks
                    SET delta_market = ratio 
                        - 
                        ( SELECT ratio
                        FROM temp_market
                        WHERE market = (SELECT market FROM STOCKS WHERE id = temp_stocks.stock_id)
                          AND temp_market.date = temp_stocks.date
                        )
                """)
    dbCon.execute("""
                       UPDATE temp_stocks
                       SET delta_zone_industry = ratio 
                           - 
                           ( SELECT ratio
                           FROM temp_zone_industry
                           WHERE zone = (SELECT zone FROM STOCKS WHERE id = temp_stocks.stock_id) 
                             AND industry = (SELECT industry FROM STOCKS WHERE id = temp_stocks.stock_id)
                             AND temp_zone_industry.date = temp_stocks.date
                           )
                   """)
    dbCon.execute("""
                       UPDATE temp_stocks
                       SET delta_zone_sector = ratio 
                           - 
                           ( SELECT ratio
                           FROM temp_zone_sector
                           WHERE zone = (SELECT zone FROM STOCKS WHERE id = temp_stocks.stock_id) 
                             AND sector = (SELECT sector FROM STOCKS WHERE id = temp_stocks.stock_id)
                             AND temp_zone_sector.date = temp_stocks.date
                           )
                   """)
    dbCon.commit()


# Caclulate the coef
def updateGain(dbCon):
    dbCon.execute("""
                      UPDATE temp_stocks  
                      SET gain = (( close_plus3 - close ) + (close_plus6 - close)) / (close * 2)
                  """)
    dbCon.commit()


def updateLastCloseAndPorcDiv(dbCon):
    dbCon.execute("""UPDATE STOCKS
                    SET lastClose = (SELECT close FROM HIST WHERE HIST.stock_id = STOCKS.id ORDER BY date DESC LIMIT 1)
                    """)
    dbCon.execute("""UPDATE STOCKS
                    SET porcDividend = round((dividend * 100) / lastClose, 1)
                """)

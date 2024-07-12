from bs4 import BeautifulSoup
import sqlite3 as sl

from selenium.webdriver import ActionChains

import util
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import database


# Scrapes the prices, vol, etc from the hist page of Investing between the histDate and histDateEnd if present
def testScrapeHistPrices(dbPath, histDate: datetime, whereClause='', fromDate=False, histDateEnd: datetime = None):
    driver = util.getFirefoxDriver(visible=True)
    dbCon = sl.connect(dbPath)
    cursor = dbCon.cursor()

    stocks = cursor.execute('SELECT link, id, bestLinkVol FROM STOCKS ' + whereClause).fetchall()

    for stock in stocks:
        testTryGetOneHistData(driver, cursor, histDate, histDateEnd, stock, fromDate, dbCon)
        break

    dbCon.close()
    driver.quit()


def testTryGetOneHistData(driver, cursor, histDate, histDateEnd, stock, fromDate, dbCon):
    stockLink = stock[0]
    stockId = stock[1]
    bestLinkVol = stock[2]
    histDate = datetime.combine(histDate.date(), datetime.min.time())
    driver.get(util.getSubLinkInvesting(stockLink + str(bestLinkVol or ''), '-historical-data'))
    soupHist = BeautifulSoup(driver.page_source, 'html.parser')
    tableHist = soupHist.find("table", class_="w-full text-xs leading-4 overflow-x-auto freeze-column-w-1")
    if tableHist is None:
        print('stock ' + str(stockId) + ' has no table curr_table')
        raise Exception('No curr_table, probably misload')
    min_Date_str = tableHist.find_all("tr")[-1].find_all("td")[0].get_text()
    min_Date = datetime.strptime(min_Date_str, '%m/%d/%Y')

    testSetHistStartDate(driver, histDate, histDateEnd)


def testSetHistStartDate(driver, histDateTime, histDateTimeEnd=None):
    iAccept = util.find_element_or_None(driver, By.ID, "onetrust-accept-btn-handler")
    if iAccept is not None:
        util.scriptClick(driver, iAccept)
    buttonDateRange = util.find_element_or_None(driver, By.CLASS_NAME, "DatePickerWrapper_icon__Qw9f8")
    util.scriptClick(driver, buttonDateRange)
    boxFromDate = util.find_element_or_None(driver, By.CLASS_NAME, "NativeDateInput_root__wbgyP")
    if boxFromDate is not None:
        buttonFromDate = boxFromDate.find_element(By.TAG_NAME, "input")
        if buttonFromDate is not None:
            util.scroll_to_element(driver, boxFromDate)
            util.scriptClick(driver, buttonFromDate)
            # util.jsClickRightSide(driver, boxFromDate)
            # buttonFromDate.send_keys(Keys.SPACE)
            action = ActionChains(driver)
            w, h = buttonFromDate.size['width'], buttonFromDate.size['height']
            x, y = buttonFromDate.location['x'], buttonFromDate.location['y']
            wx, wy = driver.get_window_size()['width'], driver.get_window_size()['height']
            action.move_to_element_with_offset(buttonFromDate, 10, 10)
            action.click()
            action.perform()
            buttonFromDate.send_keys("1")
            # date = "01/01/2023"
            # driver.execute_script('arguments[0].value=arguments[1]', buttonFromDate, date)
            # buttonFromDate.send_keys(Keys.ARROW_UP)
            # buttonFromDate.send_keys(Keys.ENTER)

    applyButton = util.find_element_or_None(driver, By.CLASS_NAME, "inv-button HistoryDatePicker_apply-button__fPr_G")
    if applyButton is not None:
        util.scriptClick(driver, applyButton)
    time.sleep(20)
    return True


# Scrapes the prices, vol, etc from the hist page of Investing between the histDate and histDateEnd if present
def scrapeHistPrices(dbPath, histDate: datetime, stock_id_min=0, stock_id_max=0, fromDate=False, histDateEnd: datetime = None):
    driver = util.getFirefoxDriver()
    dbCon = sl.connect(dbPath)
    cursor = dbCon.cursor()
    whereClauseHist = " WHERE date >= '{}'".format(histDate.strftime("%Y-%m-%d"))
    whereClauseStocks = " WHERE 1=1 "
    if stock_id_min != 0 :
        whereClauseStocks += ' AND id >= {}'.format(stock_id_min)
        whereClauseHist += ' AND stock_id >= {}'.format(stock_id_min)
    if stock_id_max != 0:
        whereClauseStocks += ' AND id <= {}'.format(stock_id_max)
        whereClauseHist +=  ' AND stock_id <= {}'.format(stock_id_max)

    cursor.execute('DELETE FROM HIST ' + whereClauseHist)
    dbCon.commit()
    stocks = cursor.execute('SELECT link, id, bestLinkVol FROM STOCKS ' + whereClauseStocks).fetchall()

    for stock in stocks:
        tryGetOneHistData(driver, cursor, histDate, histDateEnd, stock, fromDate, dbCon)

    dbCon.close()
    driver.quit()


def tryGetOneHistData(driver, cursor, histDate, histDateEnd, stock, fromDate, dbCon):
    stockLink = stock[0]
    stockId = stock[1]
    bestLinkVol = stock[2]
    histDate = datetime.combine(histDate.date(), datetime.min.time())
    doItAgain = True
    counter = 0
    while doItAgain:
        try:
            driver.get(util.getSubLinkInvesting(stockLink + str(bestLinkVol or ''), '-historical-data'))
            soupHist = BeautifulSoup(driver.page_source, 'html.parser')
            tableHist = soupHist.find("table", class_="freeze-column-w-1 w-full overflow-x-auto text-xs leading-4")

            if tableHist is None:
                print('stock ' + str(stockId) + ' has no table curr_table')
                raise Exception('No curr_table, probably misload')
            min_Date_str = tableHist.find_all("tr")[-1].find_all("td")[0].get_text()
            min_Date = datetime.strptime(min_Date_str, '%m/%d/%Y')

            if fromDate:
                if histDate < min_Date:
                    setHistStartDate(driver, histDate, histDateEnd)
                    soupHist = BeautifulSoup(driver.page_source, 'html.parser')
                    tableHist = soupHist.find("table", attrs={"id": "curr_table"})

            if tableHist is None:
                print('stock ' + str(stockId) + ' has no table curr_table')
                raise Exception('No curr_table, probably misload')
            print(stockId)
            # the first row has the column names, we reverse it to get the dates in growing order
            for rowHist in reversed(tableHist.find_all("tr")[1:]):
                tdsHist = rowHist.find_all("td")
                date_str = tdsHist[0].get_text()
                try:
                    date = datetime.strptime(date_str, '%m/%d/%Y')
                except ValueError:
                    break  # probably no data for those dates
                histDateSqlite = date.strftime("%Y-%m-%d")
                prevWorkDay = util.getPrevWorkday(date).strftime("%Y-%m-%d")
                # If we want all the prices from a date, we write all, if not, just the one selected
                if (fromDate and date >= histDate) or date == histDate:
                    close = tdsHist[1].get_text().replace(',', '')
                    high = tdsHist[3].get_text().replace(',', '')
                    low = tdsHist[4].get_text().replace(',', '')
                    change = 0
                    volume = util.numberPostFix2Number(tdsHist[5].get_text())
                    cursor.execute("""
                           INSERT OR REPLACE INTO HIST (stock_id, date, close, high, low, change, volume)
                           VALUES ( ?, ?, ?, ?, ?, ?, ?)
                           """, (stockId, histDateSqlite, close, high, low, change, volume))
                    cursor.execute("""
                        UPDATE HIST
                            SET change = 
                                IFNULL(ROUND(close - (SELECT close FROM HIST H2 WHERE date < ? 
                                                           AND H2.stock_id = HIST.stock_id
                                                           ORDER BY date DESC
                                                           LIMIT 1) 
                                     ,2) , 0)
                        WHERE stock_id = ? AND date = ?
                                   """, (histDateSqlite, stockId, histDateSqlite))
                    dbCon.commit()
                    if not fromDate:
                        break
            doItAgain = False
        except Exception as e:
            # Exceptions are likely by misload of web page, so we wait and repeat
            print("Exception: ", e, " for id: ", stockId, "repeat num: ", counter)
            counter = counter + 1
            time.sleep(60 * counter)
            if counter > 3:
                doItAgain = False


def setHistStartDate(driver, histDateTime, histDateTimeEnd=None):
    buttonDateRange = util.find_element_or_None(driver, By.ID, "widgetFieldDateRange")
    util.scriptClick(driver, buttonDateRange)

    startDateEventBox = util.find_element_or_None(driver, By.ID, "startDate")
    if startDateEventBox is None:
        return False

    util.scriptClick(driver, startDateEventBox)
    startDateEventBox.clear()

    startDateEventBox.send_keys(histDateTime.strftime("%m/%d/%Y"))

    if histDateTimeEnd is not None:
        endDateEventBox = util.find_element_or_None(driver, By.ID, "endDate")
        if endDateEventBox is None:
            return False

        util.scriptClick(driver, endDateEventBox)
        endDateEventBox.clear()

        endDateEventBox.send_keys(histDateTimeEnd.strftime("%m/%d/%Y"))

    applyButton = util.find_element_or_None(driver, By.ID, "applyBtn")
    util.scriptClick(driver, applyButton)
    time.sleep(2)
    return True

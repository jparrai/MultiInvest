from globals import *
from bs4 import BeautifulSoup
import sqlite3 as sl

import util
import database
from datetime import date


# Scrape and update the folume for a zone and date, for stocks with a bestLinkVol,
# where most volume for that stock is to be found
def scrapeVolumeBadIndex(driver, cursor, hist_date):
    cursor.execute("SELECT id, link, bestLinkVol FROM STOCKS WHERE bestLinkVol IS NOT NULL")
    stocks = cursor.fetchall()
    for stock in stocks:
        driver.get(util.getSubLinkInvesting(stock['link'] + stock['bestLinkVol']))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        volume = util.getValueStockPage(soup, "volume")
        cursor.execute('UPDATE HIST SET volume = ? WHERE stock_id = ? AND date = ?',
                       (volume, stock['id'], hist_date))


def scrapeCurrentPricesMain(driver, cursor, stock_id, link):
    try:
        driver.get(util.getSubLinkInvesting(link))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        close = util.getValueStockPage(soup, 'instrument-price-last').replace(',', '')
        low, high = util.getValueStockPage(soup, 'range-value').replace(',', '').split('-')
        volume = util.getValueStockPage(soup, 'volume').replace(',', '')
        change = util.getValueStockPage(soup, 'instrument-price-change').replace(',', '')
        cursor.execute("""
                INSERT OR REPLACE INTO HIST (stock_id, date, close, high, low, change, volume) 
                VALUES ( ?, CURRENT_DATE, ?, ?, ?, ?, ?)
                """, (stock_id, close, high, low, change, volume))
    except Exception as e:
        print('Error in scrapeCurrentPricesMain: ', e, " stock_id:", stock_id)


# Scrapes the current close, high, low, change and volume from an index page
def scrapeCurrentPricesIndex(zone):
    dbCon = sl.connect(DB_PATH)
    dbCon.row_factory = sl.Row  # this for getting the column names
    # database.fillCoefTables(dbCon, date.today())
    cursor = dbCon.cursor()

    invPage = PAGE_STOXX_600 if zone == ZONE_EU else PAGE_SP_500_INVESTING
    driver = util.getFirefoxDriver()
    driver.get(invPage)
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    table = soup.find("table", attrs={"id": "cr1"})

    for row in table.find_all("tr")[1:]:  # The first "tr" contains the field names.
        tds = row.find_all("td")

        stockName = tds[1].get_text()
        stockUrl = util.stripCid(tds[1].find("a", href=True)['href'])
        close = tds[2].get_text().replace(',', '')
        high = tds[3].get_text().replace(',', '')
        low = tds[4].get_text().replace(',', '')
        change = tds[5].get_text().replace(',', '')
        volume = util.numberPostFix2Number(tds[7].get_text())

        cursor.execute("SELECT id, bestLinkVol FROM STOCKS WHERE link = ?", (stockUrl,))
        data = cursor.fetchone()

        # if the link does not exist, we create a new stock row
        if data is None:
            cursor.execute('INSERT INTO STOCKS (name, link) VALUES (?, ?)', (stockName, stockUrl))
            stockId = cursor.lastrowid
            stockBestVolLink = ''
        else:
            stockId = data[0]
            stockBestVolLink = str(data[1] or '')

        if stockBestVolLink == '':
            cursor.execute("""
                INSERT OR REPLACE INTO HIST (stock_id, date, close, high, low, change, volume) 
                VALUES ( ?, CURRENT_DATE, ?, ?, ?, ?, ?)
                """, (stockId, close, high, low, change, volume))

        # Recalculate the average volume for all hist rows with current date
        histDateSqlite = date.today().strftime("%Y-%m-%d")

    dbCon.commit()
    dbCon.close()
    driver.quit()


# Scrapes the current prices from an index page
def scrapeCurrentPricesMainSelect(select):
    dbCon = sl.connect(DB_PATH)
    dbCon.row_factory = sl.Row  # this for getting the column names

    # database.fillCoefTables(dbCon, date.today())
    cursor = dbCon.cursor()
    cursor.execute(select)
    stocks = cursor.fetchall()
    driver = util.getFirefoxDriver()

    for stock in stocks:
        scrapeCurrentPricesMain(driver, cursor, stock['id'],
                                stock['link'] + str(stock['bestLinkVol'] or ''))

    dbCon.commit()
    dbCon.close()
    driver.quit()


def updateStocksSectors(dbPath):
    dbCon = sl.connect(dbPath)
    driver = util.getFirefoxDriver()
    dbCon.row_factory = sl.Row  # this for getting the column names

    cursor = dbCon.cursor()
    rows = cursor.execute("SELECT id,name,link FROM SECTORS").fetchall()

    for row in rows:
        print(row['link'])
        if row['link'] is not None:
            driver.get(row['link'])
            soupSector = BeautifulSoup(driver.page_source, 'html.parser')
            table = soupSector.find("table", attrs={"id": "cr1"})

            for tableRow in table.find_all("tr")[1:]:  # The first "tr" contains the field names.
                tds = tableRow.find_all("td")
                stockUrl = util.stripCid(tds[1].find("a", href=True)['href'])
                cursor.execute("SELECT id FROM STOCKS WHERE link = ?", (stockUrl,))
                data = cursor.fetchone()
                if data is not None:
                    cursor.execute('UPDATE STOCKS SET sectorOld = ? WHERE id = ?', (row['id'], data[0]))
                    dbCon.commit()
    dbCon.close()
    driver.quit()


def updateSectors(dbPath, whereClause):
    dbCon = sl.connect(dbPath)
    cursor = dbCon.cursor()
    driver = util.getFirefoxDriver()

    links = cursor.execute("SELECT link, id, sector FROM STOCKS " + whereClause).fetchall()

    for link in links:
        driver.get(util.getSubLinkInvesting(link[0], '-company-profile'))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        profile = soup.find("div", attrs={"class": "companyProfileHeader"})
        for profileUnit in profile.find_all("div"):
            if profileUnit is None:
                print(link)
            else:
                if profileUnit.get_text()[:8] == "Industry":
                    industryName = profileUnit.get_text()[8:]
                    industryUrl = profileUnit.find("a", href=True)['href']
                    industry = industryUrl.split('industry::', 1)[-1].split('|')[0]
                if profileUnit.get_text()[:6] == "Sector":
                    sectorName = profileUnit.get_text()[6:]
                    sectorUrl = profileUnit.find("a", href=True)['href']
                    sector = sectorUrl.split('sector::', 1)[-1].split('|')[0]
                    cursor.execute('UPDATE STOCKS SET sector = ?, industry = ?'
                                   'WHERE id = ?', (sector, industry, link[1]))
                    cursor.execute('INSERT OR IGNORE INTO SECTORS (id, name) VALUES (?,?)  '
                                   , (sector, sectorName))
                    cursor.execute('INSERT OR IGNORE INTO INDUSTRIES (id, name) VALUES (?,?) ', (industry, industryName))

                    dbCon.commit()
                    print(link[1])
    dbCon.close()
    driver.quit()


def getSectorId(cursor, sectorName):
    cursorId = cursor.execute("SELECT id FROM SECTORS WHERE name = '" + sectorName + "'").fetchall()
    return cursorId


def updateCurrencies(dbPath):
    BASE_URL = 'https://www.investing.com/currencies/eur-'
    dbConn = util.getDbConn(dbPath)
    cursor = dbConn.cursor()
    driver = util.getFirefoxDriver()

    rows = cursor.execute("SELECT id FROM CURRENCIES WHERE id != 'EUR'").fetchall()
    for row in rows:
        currency = row['id']
        url = BASE_URL + currency.lower()
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        price_last = float(soup.find("span", attrs={"data-test": "instrument-price-last"}).get_text())
        if currency.lower() == 'gbp':
            price_last = round((1 / price_last) / 100, 3)
        else:
            price_last = round(1 / price_last, 3)
        cursor.execute("UPDATE CURRENCIES SET changeEur = ? WHERE id = ?", (price_last, currency))
        print(str(currency) + ' ' + str(price_last))
    dbConn.commit()
    dbConn.close()
    driver.quit()


# Get stock data
def updateAllDataStocks_Ant(dbPath, whereClause):
    dbConn = sl.connect(dbPath)
    dbConn.row_factory = sl.Row  # this for getting the column names
    cursor = dbConn.cursor()
    driver = util.getFirefoxDriver()

    rows = cursor.execute("SELECT id,name,link FROM STOCKS " + whereClause).fetchall()
    if rows is None:
        print("No hay stocks que cumplan la cláusula: " + whereClause)
        return
    for row in rows:
        driver.get(util.getSubLinkInvesting(row['link']))
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        eps = util.getValueStockPage(soup, "eps")
        marketCap = util.getValueStockPage(soup, "marketCap")
        per = util.getValueStockPage(soup, "ratio")
        dividend = util.getValueStockPage(soup, "dividend")
        beta = util.getValueStockPage(soup, "beta")
        cursor.execute('UPDATE STOCKS SET per = ?, eps = ?, beta = ?, dividend = ?, marketCap = ? WHERE id = ?',
                       (per, eps, beta, dividend, marketCap, row['id']))
        dbConn.commit()
        print(row['name'])
    dbConn.close()
    driver.quit()


def updateAllDataStocks(dbPath, whereClause):
    dbConn = sl.connect(dbPath)
    dbConn.row_factory = sl.Row  # this for getting the column names
    cursor = dbConn.cursor()
    driver = util.getFirefoxDriver()

    rows = cursor.execute("SELECT id,name,link FROM STOCKS " + whereClause).fetchall()
    if rows is None:
        print("No hay stocks que cumplan la cláusula: " + whereClause)
        return
    for row in rows:
        updateStockData(dbConn, driver, row['id'], row['link'])
        print(row['name'])
    dbConn.close()
    driver.quit()


def updateNewStocks(dbPath, whereClause):
    dbConn = sl.connect(dbPath)
    dbConn.row_factory = sl.Row  # this for getting the column names
    cursor = dbConn.cursor()
    driver = util.getFirefoxDriver()

    rows = cursor.execute("SELECT id,name,link FROM STOCKS " + whereClause).fetchall()
    if rows is None:
        print("No hay stocks en: " + whereClause)
        return
    for row in rows:
        driver.get(util.getSubLinkInvesting(row['link']))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        try:
            profile = soup.find("div", attrs={"class": "instrument-metadata_currency__3RQjH"})
            allProfile = profile.find_all("span")
            currency = allProfile[1].text
            zone = 'US' if currency == 'USD' else "EU"
            profileMarket = soup.find("div", attrs={"class": "instrument-metadata_currency__3RQjH"})
            market = soup.find("a", attrs={"data-test": "instrument-bottom-market-link"}, href=True)['href']
        except:
            currency = ''
            zone = ''
            market = ''
        cursor.execute('UPDATE STOCKS SET currency = ?, zone = ? WHERE id = ?',
                       (currency, zone, row['id']))
        dbConn.commit()
        print(row['name'])

    dbConn.close()
    driver.quit()
    updateAllDataStocks(dbPath, whereClause)
    updateSectors(dbPath, whereClause)


def calculateCoefs():
    dbCon = sl.connect(DB_PATH)
    database.fillCoefTables(dbCon, date.today(), 60)
    dbCon.close()


def updateAllStocksLastCloseAndPorcDiv():
    dbCon = sl.connect(DB_PATH)
    database.updateLastCloseAndPorcDiv(dbCon)
    dbCon.close()


# Add a new stock with a given path of investing.com
# the path is given manually
def addNewStock(investingPath):

    dbConn = sl.connect(DB_PATH)
    dbConn.row_factory = sl.Row  # this for getting the column names
    cursor = dbConn.cursor()
    avgVol, beta, currency, dividend, eps, market, marketCap, name, per, zone = getStockData(investingPath)

    cursor.execute('INSERT INTO STOCKS (currency, zone, market, link, name, per , eps , beta, dividend, '
                   'marketCap, averageVol) VALUES (?,?,?,?,?,?,?,?,?,?)',
                   (currency, zone, market, investingPath, name, per, eps, beta, dividend, marketCap, avgVol))
    dbConn.commit()
    print(cursor.lastrowid)
    dbConn.close()


def updateStockData(dbConn, driver, id, investingPath):
    cursor = dbConn.cursor()
    avgVol, beta, currency, dividend, eps, market, marketCap, name, per, zone = getStockDataFromDriver(driver, investingPath)

    cursor.execute('UPDATE STOCKS SET currency = ?, zone = ?, market = ?, per = ?, eps = ?, beta = ?, dividend = ?, '
                   'marketCap = ?, avgVol = ? WHERE id = ?',
                   (currency, zone, market, per, eps, beta, dividend, marketCap, avgVol, id))
    dbConn.commit()


def getStockData(driver, investingPath):
    try:
        driver = util.getFirefoxDriver()
        avgVol, beta, currency, dividend, eps, market, marketCap, name, per, zone = getStockDataFromDriver(driver,
                                                                                                           investingPath)
    except:
        avgVol = beta = dividend = eps = marketCap = per = 0
        currency = market = name = zone = ''
    driver.quit()
    return avgVol, beta, currency, dividend, eps, market, marketCap, name, per, zone


def getStockDataFromDriver(driver, investingPath):
    try:
        driver.get(util.getSubLinkInvesting(investingPath))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        name = soup.find("h1", class_="mb-2.5 text-left text-xl font-bold leading-7 text-[#232526] md:mb-2 md:text-3xl "
                                      "md:leading-8 rtl:soft-ltr").text
        profile = soup.find("div", class_="flex items-center pb-0.5 text-xs/5 font-normal")
        currency = soup.find("span", class_="ml-1.5 font-bold").text
        zone = 'US' if currency == 'USD' else "EU"
        market = soup.find("span", class_="flex-shrink overflow-hidden text-ellipsis text-xs/5 font-normal").text
        eps = util.getValueStockPage(soup, "eps")
        marketCap = util.getValueStockPage(soup, "marketCap")
        per = util.getValueStockPage(soup, "ratio")
        dividend = util.getValueStockPage(soup, "dividend")
        beta = util.getValueStockPage(soup, "beta")
        avgVol = util.getValueStockPage(soup, "avgVolume")
    except:
        avgVol = beta = dividend = eps = marketCap = per = 0
        currency = market = name = zone = ''
    return avgVol, beta, currency, dividend, eps, market, marketCap, name, per, zone

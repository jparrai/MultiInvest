# Convert a number of the form 3.21K or 3.2198M to 3210, 3219800
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
import time
import sqlite3 as sl

from selenium.webdriver.support.wait import WebDriverWait

import sqliteUtil


def getDbConn(dbPath):
    dbConn = sl.connect(dbPath)
    dbConn.row_factory = sl.Row  # this for getting the column names
    return dbConn


def numberPostFix2Number(postFix):
    postFix = postFix.replace(',', '')
    lastChar = postFix[-1:]
    if lastChar == 'B':
        multiplier = 1000000000
    elif lastChar == 'M':
        multiplier = 1000000
    elif lastChar == 'K':
        multiplier = 1000
    else:
        multiplier = 1
    number = postFix if lastChar.isdigit() else postFix[:-1]
    number = number if len(number) > 0 else '0'
    return float(number) * multiplier


# Convert a relative link to a link to an absolute link to the historical data page in investing.com
# the possible parameters after the question mark have to go to the end of the absolute link too
def getSubLinkInvesting(rel_link, sub_link=''):
    BASE_HIST_INVESTING = 'https://www.investing.com'
    indexStartParameters = rel_link.find('?')
    if indexStartParameters == -1:
        # No parameters, just add the suffix
        return BASE_HIST_INVESTING + rel_link + sub_link
    else:
        # Parameters, the parameters must be saved
        return BASE_HIST_INVESTING + \
               rel_link[: indexStartParameters] + \
               sub_link + \
               rel_link[indexStartParameters:]


def stripCid(link):
    cidIndex = link.find('?cid=')
    if cidIndex == -1:
        return link
    else:
        return link[:cidIndex]


def getFirefoxDriver(visible=False):
    fireFoxOptions = webdriver.FirefoxOptions()
    fireFoxOptions.headless = not visible
    driver = webdriver.Firefox(options=fireFoxOptions)
    driver.implicitly_wait(10)
    return driver


def getPrevWorkday(day: datetime):
    lastBusDay = day - timedelta(days=1)
    if datetime.weekday(lastBusDay) == 5:  # if it's Saturday
        lastBusDay = lastBusDay - timedelta(days=1)  # then make it Friday
    elif datetime.weekday(lastBusDay) == 6:  # if it's Sunday
        lastBusDay = lastBusDay - timedelta(days=2)  # then make it Friday
    return lastBusDay


# Returns the date of the next given weekday after
#     the given date. For example, the date of next Monday.
#   days go from 1 (monday) to 7 (sunday)
#
def getNextWeekday(currDate, day):
    days = (day - currDate.isoweekday()-1) % 7 + 1
    return currDate + timedelta(days=days)


def find_element_or_None(driver, by, to_find):
    try:
        return driver.find_element(by, to_find)
    except NoSuchElementException:
        return None


def scroll_to_element(passed_in_driver, element):
    x = element.location['x']
    y = element.location['y']
    scroll_by_coord = 'window.scrollTo(%s,%s);' % (x, y)
    scroll_nav_out_of_way = 'window.scrollBy(0, -220);'
    passed_in_driver.execute_script(scroll_by_coord)
    passed_in_driver.execute_script(scroll_nav_out_of_way)
    time.sleep(1)


def scriptClick(driver, element_to_click):
    driver.execute_script("arguments[0].click();", element_to_click)


def scriptClickRightSide(driver, element_to_click):
    action = ActionChains(driver)
    w, h = element_to_click.size['width'], element_to_click.size['height']
    x, y = element_to_click.location['x'], element_to_click.location['y']
    wx, wy = driver.get_window_size()['width'], driver.get_window_size()['height']
    scroll_to_element(driver, element_to_click)
    action.move_to_element_with_offset(element_to_click, w - 10, h - 7)
    action.click()
    action.perform()


def jsClickRightSide(driver, element_to_click):
    w, h = element_to_click.size['width'], element_to_click.size['height']
    js_func = """
         function clickOnElem(elem, offsetX, offsetY) {
            var rect = elem.getBoundingClientRect(),
            posX = rect.left, posY = rect.top; // get elems coordinates
            // calculate position of click
            if (typeof offsetX == 'number') 
                posX += offsetX;
            else if (offsetX == 'center') {
                posX += rect.width / 2;
                if (offsetY == null) posY += rect.height / 2;
            }
            if (typeof offsetY == 'number') posY += offsetY;
            // create event-object with calculated position
            var evt = new MouseEvent('click', {bubbles: true, clientX: posX, clientY: posY});    
            elem.dispatchEvent(evt); // trigger the event on elem
        };
        alert(arguments[0] + ", " + arguments[1] + ", " + arguments[2]);
        clickOnElem(arguments[0], arguments[1], arguments[2]);
    """
    js_func = """
        alert(arguments[0] + ", " + arguments[1] + ", " + arguments[2])
    """
    # driver.execute_script('alert(arguments[0] + " " + arguments[1]);', 'KK', 'hi')

    driver.execute_script(js_func, element_to_click, w - 10, h - 10)


def getValueStockPage(soup, key, text=False):
    try:
        profile = soup.find(True, attrs={"data-test": key})
        if profile is None:
            return None
        if profile.name != 'dd':
            return profile.text
        allProfile = profile.find_all("span")
        if allProfile is not None:
            value = allProfile[1].text if len(allProfile) > 1 else ''
            multiChar = allProfile[2].text if len(allProfile) > 2 else ''
            value = value if text else numberPostFix2Number(value + multiChar)
        else:
            value = profile.text
    except Exception as e:
        print('Error in getValueStockPage: ', e)
        return None
    return str(value)

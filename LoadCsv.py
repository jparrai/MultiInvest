from globals import *
import csv
import sqlite3
import difflib

dbConn = sqlite3.connect(DB_PATH)

cursor = dbConn.cursor()

# Opening the person-records.csv file
file = open("E:/Des/Stocks/db/Sectors/tabula-STXLUXP.csv")
SECTOR_ID = 901
# Reading the contents of the
# person-records.csv file
contents = csv.reader(file)

# SQL query to insert data into the
# person table
insert_records = """INSERT INTO SECTORS_INDEXES ("zone", "sector", "name_in_pdf",	"country", "percentage"	)
                    VALUES('EU', ?, ?, ?, ?)
                  """
# Importing the contents of the file
# into our table
# cursor.executemany(insert_records, [[ SECTOR_ID, x[0], x[2], x[3]] for x in contents])

# SQL query to retrieve all data from
# the person table To verify that the
# data of the csv file has been successfully
# inserted into the table
select_all = "SELECT name_in_pdf, ROWID FROM SECTORS_INDEXES WHERE sector = " + str(SECTOR_ID)
rows = cursor.execute(select_all).fetchall()
select_stocks = "SELECT name, id FROM STOCKS WHERE zone = 'EU'"
stocks = cursor.execute(select_stocks).fetchall()

# best_matches =
# Output to the console screen
# for stock in stocks:
#    print(stock)
stocks_list = [str(stock[0]) for stock in stocks]
for row in rows:
    best_match_list = difflib.get_close_matches(row[0].lower(), [stock.lower() for stock in stocks_list])
    best_match = best_match_list[0] if any(best_match_list) else 0
    best_match_id = [stock[1] for stock in stocks if stock[0].lower() == best_match]
    print( best_match, best_match_id )
    if any(best_match_id):
        update_query = 'UPDATE SECTORS_INDEXES SET stock_id = ' + str(best_match_id[0]) + \
                       ' WHERE ROWID = ' + str(row[1])
        cursor.executescript(update_query)
        print(update_query)
# Committing the changes
dbConn.commit()

# closing the database connection
dbConn.close()

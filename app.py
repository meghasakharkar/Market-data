import os, sys, zipfile, MySQLdb
import pandas as pd

file_path = 'D:\PycharmWS\data-files\stockdata.zip'

#     if file not present exit the program
if not (os.path.isfile(file_path)):
    print("file not available")
    os.exit(0)

#      unzip the folder & read files
zp = zipfile.ZipFile(file_path)
name = zp.namelist()

#      Connect with database
con = MySQLdb.connect(host='localhost', user='root', password='megha', db='market_data')

#      check connection establish or not
if con != None:
    print("database connected")
else:
    print("could not connect to database")
    con.close()
    os.sys(0)

cursor = con.cursor()

#       read each csv file & store data in database
try:
    for data_file in name:
        csv_file = pd.read_csv(zp.open(data_file))

#        col_name=csv_file.iloc[2][2]
#        print(col_name)

        for index, row in csv_file.iterrows():
            cursor.execute('insert into nse_equity_ts(open_price,high_price,low_price,close_price) '
                           'values("%s","%s","%s","%s")',
                           (row["Open Price"], row["High Price"], row["Low Price"], row["Close Price"]))
        con.commit()

        print('all records are inserted')
except MySQLdb.DatabaseError as e:
    if con:
        con.rollback()
        print("there is a problem", e)
finally:
    #      close database connection after successfully inserting data
    if cursor:
        cursor.close()
    if con:
        con.close()

import sqlite3

con = sqlite3.connect("shioaj.db")
cursor = con.cursor()
sql = '''CREATE TABLE IF NOT EXISTS  stock(
   stock_id CHAR(10) NOT NULL,
   date CHAR CHAR(10) NOT NULL,
   PRICE INT);'''

cursor.execute(sql)
con.commit()
cursor.close()
con.close()





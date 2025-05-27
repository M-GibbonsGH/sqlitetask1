# Using sqlite3



import sqlite3
import pandas as pd

df = pd.read_csv("sqlitetask1/shoes.csv")                               # Create pandas DataFrame out of a csv file

conn = sqlite3.connect("shoes.db")                          # Create connection to an sqlite database (if none with that name, it's created)

df.to_sql("shoes_table", conn, if_exists='replace')          # Write the DataFrame data to a table in the SQL database. If table exists already, modify it rather than replace it.


cursor = conn.cursor()                                       # To modify the table (rather than just query it), we need to use a new class instance, cursor

# Below, inserting 20 new rows into the table, and populating a single column with the same value for each row. Recursive common table expression.

cursor.execute("""WITH RECURSIVE rec_cte(n) AS (select 1 union all select n + 1 from rec_cte where n < 20) INSERT INTO shoes_table (Brand) SELECT "Topo" from rec_cte""")
                                                        # IMPORTANT: two different syntaxes appear to work fine: 'instantiating' the cte can come before OR after the insert clause

conn.commit()


# Below I'm modifying (updating) existing rows, not adding new ones. So no recursive CTE needed: instead an UPDATE clause
# LIMIT clause works in MySQL, but NOT in Postgres or SQLite. For the latter two, you can instead use subclause "where rowid in (select ...)" 

cursor.execute("""UPDATE shoes_table SET Model = "Atmos" WHERE ROWID IN (SELECT ROWID FROM shoes_table WHERE Brand = "Topo" LIMIT 10)""");


cursor.execute("UPDATE shoes_table SET Sex = 'Female' WHERE ROWID IN (SELECT ROWID FROM shoes_table WHERE Brand = 'Topo' LIMIT 5)")

cursor.execute("UPDATE shoes_table SET Sex = 'Male' WHERE ROWID IN (SELECT ROWID FROM shoes_table WHERE Brand = 'Topo' LIMIT 5 OFFSET 5)")

conn.commit()


# Statement == one SQL command
# Transaction == grouping of multiple SQL statments that sqlite will treat as a single unit

# seems that you cannot have Offset by itself: must be Limit...Offset, otherwise error 
cursor.execute("UPDATE shoes_table SET Model = 'Ultrafly' WHERE ROWID IN (SELECT ROWID FROM shoes_table WHERE BRAND = 'Topo' LIMIT 10 OFFSET 10)");
               
cursor.execute("UPDATE shoes_table SET Sex = 'Female' WHERE ROWID IN (SELECT ROWID FROM shoes_table WHERE Brand = 'Topo' LIMIT 5 OFFSET 10)");
               
cursor.execute("UPDATE shoes_table SET Sex = 'Male' WHERE ROWID IN (SELECT ROWID FROM shoes_table WHERE Brand = 'Topo' LIMIT 5 OFFSET 15)");


cursor.execute("WITH RECURSIVE rec_cte2(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM rec_cte2 WHERE x < 30) INSERT INTO shoes_table (Brand) SELECT 'True Motion' from rec_cte2")

conn.commit()

q = pd.read_sql_query("SELECT * FROM shoes_table", conn)
print(q)

conn.close()

# so next to find out: why are the added rows not indexed? They should be
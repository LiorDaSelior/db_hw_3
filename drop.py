import mysql.connector as mdb
import util

con = mdb.connect(host=util.HOSTNAME, port=util.PORT, database=util.DATABASE, user=util.USERNAME,
                    password=util.PASSWORD)
cur = con.cursor()
try:
    cur.execute("""
        DROP TABLE movie_role;
        DROP TABLE movie_genre;
        DROP TABLE role;
        DROP TABLE genre;
        DROP TABLE movie;
        DROP TABLE person;
        """)
except mdb.Error as e:
    print("Error:", e)
finally:
    cur.close()
    con.close()
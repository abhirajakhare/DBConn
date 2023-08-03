"""import pandas as pd"""
import oracledb 
import sys

# Test to see if it will print the version of sqlalchemy
print(oracledb.__version__)  # this returns 1.2.15 for me

# Test to see if the cx_Oracle is recognized
print(oracledb.version)  # this returns 8.0.1 for me

# This fails for me at this point but will succeed after the solution described below
#oracledb.clientversion()

try :
    conn = oracledb.connect("system/H1r0sh1ma@localhost:1521/XEPDB1")    
    print(conn.version) 
    cur = conn.cursor()
    sql = """select to_char(count(*),'FM999G999G999G999') from employee"""
    cur.execute(sql)
    rows = cur.fetchall()
    print("rows", rows)  
except oracledb.DatabaseError as exception: 
    print(exception)
finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
    sys.exit(0)
# Read data from the CSv file and
# insert into preexisting table.
# importing module
from pickle import FALSE
import cx_Oracle as CXO
import pandas as pd
from sqlalchemy import false

# Create a table in Oracle database
try:

    con = CXO.connect("system/H1r0sh1ma@localhost:1521/XEPDB1")
    print(con.version)

    # Now execute the sqlquery
    cursor = con.cursor()
    print("You're connected: ")

    # read data employee data from CSV file
    EmpData = pd.read_csv("Emp_data.csv")
    """print(EmpData.head())"""
    print("Truncate data before table table....")
    sql = "Truncate table employee"
    cursor.execute(sql)

    for i, row in EmpData.iterrows():
        sql = "INSERT INTO employee(empid, name, salary) VALUES(:1,:2,:3)"
        cursor.execute(sql,tuple(row))
        # the connection is not autocommitted by default, so we must commit to save our changes
        con.commit()
    print("Record inserted succesfully")

except CXO.DatabaseError as e:
    (err,) = e.args
    print("Oracle-Error-Code:", err.code)
    print("Oracle-Error-Message:", err.message)

# by writing finally if any error occurs
# then also we can close the all database operation
finally:
    if cursor:
        cursor.close()
    if con:
        con.close()

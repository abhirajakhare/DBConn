# importing module
import cx_Oracle

# Create a table in Oracle database
try:

    con = cx_Oracle.connect("system/H1r0sh1ma@localhost:1521/XEPDB1")
    print(con.version)

    # Now execute the sqlquery
    cursor = con.cursor()

except cx_Oracle.DatabaseError as e:
    print("There is a problem with Oracle", e)

# by writing finally if any error occurs
# then also we can close the all database operation
finally:
    if cursor:
        cursor.close()
    if con:
        con.close()

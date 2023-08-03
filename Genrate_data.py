import pandas as pd
import numpy as np
import cx_Oracle as CXO


def InConverter(value):
    return int(value)  # or whatever is needed to convert from numpy.int64 to an integer


def InputTypeHandler(cursor, value, num_elements):
    if isinstance(value, np.int32):
        return cursor.var(int, arraysize=num_elements, inconverter=InConverter)
    if isinstance(value, np.int64):
        return cursor.var(int, arraysize=num_elements, inconverter=InConverter)


# Create a Oracle database connection
try:
    # package each dataframe row into a tuple
    df = pd.read_csv("Emp_data.csv")
    df = df.head(200)
    df_records = df.to_records(index=False)
    # wrap tuples in list
    rows = list(df_records)

    con = CXO.connect("system/H1r0sh1ma@localhost:1521/XEPDB1")
    print(con.version)
    cursor = con.cursor()

    # Truncate the employee table before the load
    # Now execute the sqlquery
    sql = "Truncate table employee"
    cursor.execute(sql)
    cursor.bindarraysize = 5
    cursor.setinputsizes(int, 38, int)
    cursor.inputtypehandler = InputTypeHandler

    sql_statement = """insert into employee(empid, name, salary) VALUES(:1,:2,:3)"""
    cursor.executemany(
        sql_statement,
        rows,
        batcherrors=True,
    )
    print("inserted succesfully")
    for error in cursor.getbatcherrors():
        print("Error", error.message, "at row offset", error.offset)
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

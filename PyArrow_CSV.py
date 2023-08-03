import oracledb as ora
import pandas as pd
import pyarrow as pa
import pyarrow.csv as csv
import sys
from datetime  import datetime

chunk =1000000
# Create empty list
df = []
# Create empty dataframe
dfs = pd.DataFrame() 

# Read a table in Oracle database and write to a CSV
try:
    start_time = datetime.now()
    con = ora.connect("system/H1r0sh1ma@localhost:1521/XEPDB1")
    print(con.version + " connect as " +con.username + " to " + con.dsn )
    SQL = """select * from employee"""
    # Now execute the sqlquery
    #cursor = con.cursor()
    for chunk in pd.read_sql(SQL, con=con, chunksize=chunk):
        df.append(chunk)

    dfs = pd.concat(df, ignore_index=True)
    pa_table = pa.Table.from_pandas(dfs)
    print(pa_table.num_rows)
    # column names are kept in table metadata, so omit header
    csv.write_csv(pa_table, 'py_arrow.csv',
                  csv.WriteOptions(include_header=False))
    time_elasped = datetime.now() - start_time
    print(" Time Elasped (hh:mi:ss.ms) {}".format(time_elasped))

except ora.DatabaseError as e:
    print("There is a problem with Oracle", e)
    sys.exit(1)
# by writing finally if any error occurs
# then also we can close the all database operation
finally:
    if con:
        con.close()
    sys.exit(0)

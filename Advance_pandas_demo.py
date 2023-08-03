import sys
import oracledb as ora
import pandas as pd
from datetime import datetime

chunk = 1000000
offset = 0
# Create empty list 
df = []
# Create empty dataframe
dfs = pd.DataFrame()

try:
    start_time = datetime.now()
    conn = ora.connect("system/H1r0sh1ma@localhost:1521/XEPDB1")
    sql = """
        select * from employee
        """
    for chunk in pd.read_sql(sql, con=conn, chunksize=chunk):
        df.append(chunk)

    dfs = pd.concat(df, ignore_index=True)
    time_elasped = datetime.now() - start_time
    print(" Time Elasped (hh:mi:ss.ms) {}".format(time_elasped))
    print(dfs.size)
    print(dfs.shape())
    #dfs.to_csv("data_dump.csv")
except ora.DatabaseError as exception:
    print(exception)
finally:
    if conn:
        conn.close()
    sys.exit(0)

import sys
import csv
import oracledb as ora
from datetime import datetime
ORACLE_CONNECT = "system/H1r0sh1ma@localhost:1521/XEPDB1"
OUTPUT_ENCODING = "utf-8"
filename = "out.csv"
conn = ora.connect(ORACLE_CONNECT) 
start_time = datetime.now()
cursor = conn.cursor()
cursor.execute("select * from employee")
cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
data = cursor.fetchall()
fieldnames = [d[0] for d in cursor.description]
#print(fieldnames)
#print(data)

with open(filename, 'w',encoding='utf-8',newline='') as csv_file:
    writer = csv.DictWriter(csv_file,  extrasaction='ignore',fieldnames=fieldnames )
    writer.writerows(data)

time_elapsed = datetime.now() - start_time
print("Time Elasped (hh:mi:ss.ms) {}".format(time_elapsed))
if cursor:
    cursor.close()
if conn:
    conn.close()
sys.exit(0)        


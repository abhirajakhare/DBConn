import sys
import csv
import oracledb
from datetime import datetime

ORACLE_CONNECT = "system/H1r0sh1ma@localhost:1521/XEPDB1"
OUTPUT_ENCODING = "utf-8"
BATCH_SIZE = 250000
FILENAME = "out.csv"
SQL = """
    select * from employee
    """


def export_table_csv_format(conn, FILENAME):
    cursor = conn.cursor()
    cursor.execute(SQL)
    fieldnames = [d[0] for d in cursor.description]
    row = 0
    done = False
    while not done:  # add table rows
        cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
        row_data = cursor.fetchmany(BATCH_SIZE)
        if len(row_data) < 1:
            done = True
        else:
            row += len(row_data)
            print("{:,d}".format(row))
            cursor.rowfactory = lambda *args: dict(
                zip([d[0] for d in row_data.description], args))            
            #print(fieldnames)
            #print(row_data)
            with open(FILENAME, 'w', encoding='utf-8', newline='') as csv_file:
                writer = csv.DictWriter(csv_file,extrasaction='ignore', fieldnames=fieldnames)
                writer.writerows(row_data)


def main():
    start_time = datetime.now()
    conn = oracledb.connect(ORACLE_CONNECT)
    print("Connected to Oracle: " + oracledb.version)
    export_table_csv_format(conn, FILENAME)
    time_elapsed = datetime.now() - start_time
    print("Time Elasped (hh:mi:ss.ms) {}".format(time_elapsed))


if __name__ == "__main__":
    main()



import codecs
import csv
import oracledb
from datetime import datetime

 

ORACLE_CONNECT = "system/H1r0sh1ma@localhost:1521/XEPDB1"
OUTPUT_ENCODING = "utf-8"
BATCH_SIZE = 1000000

sql = """
select * from employee 
"""



def append_row(filename, rows):
    with codecs.open(filename, "a", OUTPUT_ENCODING) as outfile:
        output = csv.writer(outfile, dialect="excel")
        for row in rows:
            output.writerow(row)


def export_table_data(orcl, filename):
    start_time = datetime.now()
    # output each table content to a separate CSV file

    with codecs.open(filename, "wb", OUTPUT_ENCODING) as infile:
        pass

    curs2 = orcl.cursor()
    curs2.execute(sql)

    cols = []
    for col in curs2.description:
        cols.append(col[0])
    append_row(filename, [cols])

    row = 0
    done = False
    while not done:  # add table rows
        row_data = curs2.fetchmany(BATCH_SIZE)
        if len(row_data) < 1:
            done = True
        else:
            append_row(filename, row_data)
            row += len(row_data)
            print("{:,d}".format(row))
    time_elapsed =  datetime.now() - start_time
    print('Time Elasped (hh:mi:ss.ms) {}'.format(time_elapsed))


orcl = oracledb.connect(ORACLE_CONNECT)
print("Connected to Oracle: " + orcl.version)


export_table_data(orcl, "output.csv")


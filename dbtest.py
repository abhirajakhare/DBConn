import cx_Oracle as CXO
import csv

# Predefine the memory areas to match the table definition.
# This can improve performance by avoiding memory reallocations.
# Here, one parameter is passed for each of the columns.
# "None" is used for the ID column, since the size of NUMBER isn't
# variable.  The "25" matches the maximum expected data size for the
# NAME column
con = CXO.connect("system/H1r0sh1ma@localhost:1521/XEPDB1")
print(con.version)

# Now execute the sqlquery
cursor = con.cursor() 
print("You're connected: ")
cursor.setinputsizes(None, 30, None)

# Adjust the number of rows to be inserted in each iteration
# to meet your memory and performance requirements
batch_size = 10000

with open("data_dump.csv", "r") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=",")
    sql = "insert into employee (empid,name, salary) values (:1, :2, :3)"
    print(sql)
    data = []
    for line in csv_reader:
        data.append((line[0], line[1], line[2]))
        if len(data) % batch_size == 0:
            cursor.executemany(sql, data)
            data = []
        if data:
            con.commit()    

if con:
    con.close()

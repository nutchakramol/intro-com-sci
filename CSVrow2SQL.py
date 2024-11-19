import fileinput
import csv
import sys
import sqlite3
import time

# SQL insert by raw SQL
# Note: CREATE TABLE must be done before running.

TABLENAME = "T_COVID"
TEXT_ENCODING = "utf-8-sig"
SQL_FILENAME = "patient.sql"

def row2SQLinsert(tbl, row):
    row_escaped_str = ""
    for i, elem in enumerate(row):
        if i > 0:
            row_escaped_str += ","
        #print("row=",elem)
        if elem == "":
            row_escaped_str += "NULL"
        else:
            # not recommended, just understanding how the sql generated
            # SQL escaping depends on database software.
            elem = elem.replace('\n', '') # delete EOL
            elem = elem.replace('\\', '\\\\') # replace \ with \\
            elem = elem.replace('\'', '\'\'') # replace ' with ''
            elem = elem.replace('\"', '\"\"') # replace " with ""
            elem = elem.replace(',', '\,') # replace , with \,
            row_escaped_str += "'" + elem + "'"

    row_escaped_str = "INSERT INTO " + tbl + " VALUES(" + row_escaped_str + ");\n" # append ' to the head and the tail
    return row_escaped_str

if __name__ == '__main__':
    # for line in fileinput.input(encoding="utf-8"): # latest python it will work
    isfirstline = True
    # Excel CSV contains UTF-8 BOM, so use utf-8-sig instead of utf-8
    fp = open(SQL_FILENAME, "wt", encoding=TEXT_ENCODING)
    #fp.write("BEGIN TRANSACTION;\n")
    csvreader = csv.reader(fileinput.input(openhook=fileinput.hook_encoded(TEXT_ENCODING)), delimiter=',')
    for r in csvreader:
        if isfirstline == True:
            isfirstline = False
            pass
        else:
            buf = row2SQLinsert(TABLENAME, r)
            fp.write(buf)
    #fp.write("COMMIT TRANSACTION;\n")
    fp.flush()
    fp.close()


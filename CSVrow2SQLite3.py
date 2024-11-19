import fileinput
import csv
import sys
import sqlite3
import time

# SQL table creation, and insertion by parameter query

# Configurations
SQLITE3_DB = "covid19.sqlite3" # sqlite3 DB filename
TABLE_NAME = "T_COVID"         # tablename for importing data from CSV
TEXT_ENCODING = "utf-8-sig"    # Excel CSV contains UTF-8 BOM. if not necessary to consider BOM, should use "utf-8"
DEFAULT_DBTYPE = "TEXT"        # Wider datatype is better
containsHeader = True          # if header is in input, create TABLE_NAME in SQLITE3_DB


if __name__ == '__main__':
    # for line in fileinput.input(encoding="utf-8"): # latest python it will work

    conn = sqlite3.connect(SQLITE3_DB)
    cursor = conn.cursor()
    cursor.execute("BEGIN TRANSACTION;")

    start_time = time.time()

    # Excel CSV contains UTF-8 BOM, so use utf-8-sig instead of utf-8
    csvreader = csv.reader(fileinput.input(openhook=fileinput.hook_encoded(TEXT_ENCODING)), delimiter=',')

    sql_params_str = ""
    rows_list = list()
    for row in csvreader:
        if containsHeader == True:
            create_table_column_name_str = ""
            for c in row:
                if create_table_column_name_str != "":
                    create_table_column_name_str += ","
                # Warning: the two statements below depends on database
                c = c.replace("'", "''") # SQLite3 escaping for ': convert ' to ''.
                c = "'" + c + "'" # escape
                create_table_column_name_str += c + " " + DEFAULT_DBTYPE

            try:
                cursor.execute("CREATE TABLE " + TABLE_NAME + "(" + create_table_column_name_str + ")")
            except sqlite3.OperationalError as e:
                print("Warning: Table " + TABLE_NAME + " may already exists.")
                print(e)
            sql_params_str = "?," * len(row)
            sql_params_str = sql_params_str[:len(sql_params_str)-1]
            containsHeader = False
        else:
            rows_list.append(row)

    #print(rows_list)
    sql_str = "INSERT INTO " + TABLE_NAME + " VALUES(" + sql_params_str + ")"
    try:
        cursor.executemany(sql_str, rows_list)
        cursor.execute("COMMIT TRANSACTION;")
        print("CONVERTED")
    except sqlite3.OperationalError as e:
        print(e)
        print("ROLLBACKED")
        cursor.execute("ROLLBACK TRANSACTION;")
    finally:
        end_time = time.time()
        print("exec time=", end_time - start_time)
        cursor.close()
        conn.close()

# EOF



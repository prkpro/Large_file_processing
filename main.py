import csv
import multiprocessing
import os
import storage
from mysql.connector import errorcode


BATCH = 5000

class insert:

    def __init__(self, csv_file_nm, db_table_nm):
        # Connect to the database, perform the insert, and update the log table.
        self.insert_parallel(db_table_nm,csv_file_nm)
        

    def connect_db(self, sql_server_nm, db_nm):
        # Connect to the server and database.
        try:
          conn = storage.connect()
        except mysql.connector.Error as err:
          if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
          elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
          else:
            print(err)
        
        return conn

    def insert_many(self, table, cols, rows, cursor):
        sql = 'INSERT INTO `{table}` ({cols}) VALUES ({marks})'.format(
          table=table,
          cols=', '.join(cols),
          marks=', '.join(['%s'] * len(cols)))


        try:
            print("Inserting data to {}:".format(db_table_nm),end='')
            cursor.execute(sql, *rows)
        except mysql.connector.DataError as err:
            print("Data Error : {} ".format(err.msg))
        except mysql.connector.ProgrammingError  as err:
            print("Programming Error : {} ".format(err.msg))
        except mysql.connector.Error as err:
            print("Other Error : {} ".format(err.msg))

        else:
            print('process {0} inserted {1} rows into table {3}'.format(os.getpid(), len(rows), table))
        conn.commit()
        cursor.close()
        conn.close()

        
    def insert_worker(self, table, cols, queue):
        rows = []
        #  Each child process creates its own  engine  object 
        conn = self.connect_db(sql_server_nm, db_nm)
        cursor= conn.connect()
        while True:
            row = queue.get()
            if row is None:
                if rows:
                    self.insert_many(table, cols, rows, cursor)
                break

            rows.append(row)
            if len(rows) == BATCH:
                self.insert_many(table, cols, rows, cursor)
                rows = []


    def insert_parallel(self, table, reader, w=10):
        cols = ['name', 'sku', 'description']
        
        #  Data queue, the main process reads the file and writes data into it, worker  The process reads data from the queue 
        #  Pay attention to 1 Control the size of the queue to avoid too slow consumption resulting in the accumulation of too much data, too much memory 
        queue = multiprocessing.Queue(maxsize=w*BATCH*2)
        workers = []
        for i in range(w):
            p = multiprocessing.Process(target=self.insert_worker, args=(table, cols, queue))
            p.start()
            workers.append(p)
            print('starting # {0} worker process, pid: {1}...'.format(i + 1, p.pid))
        
        for line in reader:
            queue.put(tuple(line))
            
            if reader.line_num % 50000 == 0:
                print('put {} tasks into queue.'.format(reader.line_num))

        #  For each  worker  Send a signal that the task is over 
        print('send close signal to worker processes')
        for i in range(w):
            queue.put(None)

        for p in workers:
            p.join()

class update_select:
    def __init__(self, db_table_nm, column_update, update_value, sku_value,mode):
        # Connect to the database, perform the insert, and update the log table.
        conn = self.connect_db()

        if self.mode==1:
            self.update_data(conn, db_table_nm, column_update, update_value, sku_value)
            conn.close()
        if self.mode==0:
            self.select_data(conn, db_table_nm)
            conn.close()
                

    def connect_db(self):
        # Connect to the server and database with Windows authentication.
        try:
          conn = storage.connect()
        except mysql.connector.Error as err:
          if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
          elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
          else:
            print(err)
        
        return conn

    
    def update_data(self, conn, db_table_nm, column_update, update_value, sku_value):
        
        update_qry = "Update "+db_table_nm+" set "+column_update+" = "+update_value+" Where sku = "+ sku_value
        

        # Execute the query
        cursor = conn.cursor()

        try:
            print("Inserting data to {}:".format(db_table_nm),end='')
            cursor.execute(update_qry)
        except mysql.connector.DataError as err:
            print("Data Error : {} ".format(err.msg))
        except mysql.connector.ProgrammingError  as err:
            print("Programming Error : {} ".format(err.msg))
        except mysql.connector.Error as err:
            print("Other Error : {} ".format(err.msg))

        else:
            print("OK")
        conn.commit()
        cursor.close()
        conn.close()
    def select_data(self, conn, db_table_nm):
        
        select_qry = "Create table Agg AS Select Name, count(1) from "+db_table_nm+" group by Name;"
        

        # Execute the query
        cursor = conn.cursor()

        try:
            print("Inserting data to {}:".format(db_table_nm),end='')
            cursor.execute(select_qry)
        except mysql.connector.DataError as err:
            print("Data Error : {} ".format(err.msg))
        except mysql.connector.ProgrammingError  as err:
            print("Programming Error : {} ".format(err.msg))
        except mysql.connector.Error as err:
            print("Other Error : {} ".format(err.msg))

        else:
            print("OK")
        conn.commit()
        cursor.close()
        conn.close()


def main():
    print('Options:\n1.insert\n2.Update\n3.Create an aggregated table ')
    i=int(input().strip())
    
    if i == 1:
        print('Enter csv file path:')
        file_path= input().strip()
        with open(file_path) as fd:
            print('Reading...')
            fd.readline()  # skip header
            reader = csv.reader(fd)
            print('Calling insert parallely...')
            insert(reader, 'Products')
    elif i==2:
        print('Enter column name , value to update, for which sku value:')
        column, txt, sku_value = input(), input(), input()
        update_select('Products', column, txt, sku_value,mode=1)
    elif i==3:
        update_select('Products', '', '', '',mode=0)


if __name__=="__main__":
    main()

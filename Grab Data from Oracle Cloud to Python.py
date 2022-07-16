import sys
import config # config file contain all the login information and query
import cx_Oracle
import pandas as pd
import warnings

warnings.filterwarnings("ignore")

lib_dir = r"C:\Users\FCHEN\instantclient-basic-windows.x64-19.11.0.0.0dbru\instantclient_19_11"


class DBconnection:
    def __init__(self, dsn, port, service_name, username, password):
        self.dsn = dsn
        self.port = port
        self.service_name = service_name
        self.username = username
        self.password = password

        self.dsn_tns = None
        self.connection = None

    def check_Connection(self):  ##check connection of oracle_client
        try:
            cx_Oracle.init_oracle_client(
                lib_dir=r"C:\Users\FCHEN\instantclient-basic-windows.x64-19.11.0.0.0dbru\instantclient_19_11")
        except Exception as err:
            print("Error connecting: cx_Oracle.init_oracle_client()")
            print(err);
            sys.exit(1);
        return print("cx_Oracle connecting online")

    def get_conn(self):
        try:
            if self.dsn_tns is None:
                self.dsn_tns = cx_Oracle.makedsn(host=self.dsn,
                                                 port=self.port,
                                                 service_name=self.service_name)  ##create DSN login
                if self.connection is None:
                    self.connection = cx_Oracle.connect(user=self.username,
                                                        password=self.password,
                                                        dsn=self.dsn_tns,
                                                        encoding="UTF-8"
                                                        )
                    connection = self.connection

                print("success establish to database")

                return connection


        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 955:
                print('Talbe Already exists')

            if error.code == 1031:
                print("Insufficient privileges")
            print(error.code)
            print(error.message)
            print(error.context)


class DBquery:
    def __init__(self, connection,query):
        self.connection = connection
        self.query = query
    def Query(self):
        lists = []


        try:

            cursor = self.connection.cursor()
            cursor.execute(self.query)
            print("success execut query")

        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 955:
                print('Talbe Already exists')

            if error.code == 1031:
                print("Insufficient privileges")
            print(error.code)
            print(error.message)
            print(error.context)

        print("Selecting rows from student table cursor.fetchall")
        user_records = cursor.fetchall()  ##name the data taht we query
        columns = cursor.description  ##grab the headers from query

        print("Print each row and it's columns values")
        for row in user_records:  ##for each row of the data we grab from orcal
            tmp = {}  ####create an empty tuple for catching the header
            for (index, column) in enumerate(row):  ##pure magic
                tmp[columns[index][0]] = column
            lists.append(tmp)
        df = pd.DataFrame(lists)
        print(df)



        return df

    def close(self):
        cursor = self.connection.cursor()
        if self.connection:
            cursor.close()
            self.connection.close()
            print("SQL connection is closed")

import psycopg2
from psycopg2 import Error
from .utils import print_error
import sys
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import urlparse
import csv

class PostrgesDB:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(user="postgres",
                                  password="1234",
                                  host="localhost",
                                  port="5432",
                                  database="pipelines")
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print("Информация о сервере PostgreSQL")
            print(self.connection.get_dsn_parameters(), "\n")
            # Выполнение SQL-запроса
            self.cursor.execute("SELECT version();")
            # Получить результат
            record = self.cursor.fetchone()
            print("Вы подключены к - ", record, "\n")
        except (Exception, Error) as error:
            print_error(f"Ошибка PostgreSQL: {error}")
            sys.exit(1)

    def run_query(self, query):
        try:
            self.cursor.execute(query)
        except (Exception, Error) as error:
            print_error(f"Ошибка PostgreSQL: {error}")
            sys.exit(1)

    def load_data_to_table(self, input_file, table):
        df = pd.read_csv(input_file)

        df.to_sql(table, self.connection, if_exists='replace')
        sql1 = f'select * from public."{table}"'
        self.cursor.execute(sql1)
        for i in self.cursor.fetchall():
            print(i)

    def load_data_to_table_with_domain(self, input_file, table):
        df = pd.read_csv(input_file)
        df['domain_of_url'] = df['url']
        df['domain_of_url'] = df['domain_of_url'].apply((lambda x: urlparse(x).netloc))

        df.to_sql(table, self.connection, if_exists='replace')
        sql1 = f'select * from public."{table}"'
        self.cursor.execute(sql1)
        for i in self.cursor.fetchall():
            print(i)

    def copy_data_to_file(self, table, output_file):
        sql1 = f'select * from public."{table}"'
        self.cursor.execute(sql1)

        with open(output_file, 'w') as f:
            writer = csv.writer(f)
            for row in self.cursor.fetchall():
                writer.writerow(row)

    def close_connection(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
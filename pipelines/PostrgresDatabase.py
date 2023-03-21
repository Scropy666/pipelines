import psycopg2
from psycopg2 import Error
import pandas as pd
from .config import POSTGRES_PASSWORD, POSTGRES_USER, POSTGRES_DB, POSTGRES_HOST


class PostgresDB:
    def __init__(self, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port='5432'):
        try:
            self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
            self.cursor = self.conn.cursor()
            # Распечатать сведения о PostgreSQL
            print("Информация о сервере PostgreSQL")
            print(self.conn.get_dsn_parameters(), "\n")
            # Выполнение SQL-запроса
            self.cursor.execute("SELECT version();")
            # Получить результат
            record = self.cursor.fetchone()
            print("Вы подключены к - ", record, "\n")
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)

    def run_query(self, query):
        try:
            self.cursor.execute(query)
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)

    def run_query_without_output(self, query):
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)
            self.conn.commit()
            self.cursor.close()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)

    def number_of_records_after_query(self, query):
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)
            cnt = len(self.cursor.fetchall())
            self.conn.commit()
            self.cursor.close()

            return cnt
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
    def load_data_to_table(self, input_file, table_name):
        query = f"COPY {table_name} FROM STDIN DELIMITER ',' CSV HEADER"
        self.cursor.copy_expert(query, open(input_file, "r"))
    def copy_to_file(self, table_name, output_file):
        query = f"COPY (SELECT * FROM {table_name}) TO STDOUT DELIMITER ',' CSV HEADER"
        self.cursor.copy_expert(query, open(output_file, "w"))


    def create_table_domain_of_url(self):
        self.run_query("drop function if exists domain_of_url;")

        query = """
                create function domain_of_url(url text)
                returns text
                language plpgsql
                as
                $$
                declare
                   domain_of_url text;
                begin
                   select (regexp_matches(url, '\/\/(.*?)\/', 'g'))[1]
                   into domain_of_url;

                   return domain_of_url;
                end;
                $$;
                """
        self.run_query(query)
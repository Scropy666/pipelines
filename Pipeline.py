import psycopg2
import click
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import urlparse
import csv




# @click.command()
# @click.option('--uid', type=str, required=True, help='PostgreSQL uid')
# @click.option('--pwd', type=str, required=True, help='PostgreSQL password name')
# @click.option('--server', type=str, required=True, help='PostgreSQL server')
# @click.option('--port', type=str, required=True, help='PostgreSQL port')
# @click.option('--db', type=str, required=True, help='PostgreSQL database')

def get_postgres_creds(uid, pwd, server, port, db):
    cs = create_engine(f'postgresql://{uid}:{pwd}@{server}:{port}/{db}')
    try:
        return cs
    except:
        print("Error loading the config file.")


# def get_sql_conn(uid, pwd, server, port, db):
#     """return db connection."""
#     # get password from environmnet var
#     conn = psycopg2.connect(user="postgres",
#                             # пароль, который указали при установке PostgreSQL
#                             password="QWERTY345p",
#                             host="127.0.0.1",
#                             port="5432",
#                             database="Dagster")
#     try:
#         return conn
#     except:
#         print("Error loading the config file.")


def load_to_db():
    data = {'id': ['1', '2'], 'name': ['hello', 'world'], 'url': ['http://hello.com/home', 'https://world.org/']}
    df = pd.DataFrame(data)
    original_tbl = 'Test'
    engine = get_postgres_creds('postgres', 'QWERTY345p', "127.0.0.1", "5432", "Dagster")
    conn = engine.raw_connection()
    # Курсор для выполнения операций с базой данных
    conn.autocommit = True
    cursor = conn.cursor()
    # Распечатать сведения о PostgreSQL
    print("Информация о сервере PostgreSQL")
    print(conn.get_dsn_parameters(), "\n")
    # Выполнение SQL-запроса
    cursor.execute("SELECT version();")
    # Получить результат
    record = cursor.fetchone()
    print("Вы подключены к - ", record, "\n")

    df.to_sql(original_tbl, engine, if_exists='replace')

    sql1 = '''select * from public."Test";'''
    cursor.execute(sql1)
    for i in cursor.fetchall():
        print(i)

# original_tbl - таблица, в которой хранится изначальный датасет, new_table - новая таблица с дополнительной колонкой
# содержащей домен

def sql_create_table_as():

    original_tbl = 'Test'
    new_table = 'Test1'
    engine = get_postgres_creds('postgres', 'QWERTY345p', "127.0.0.1", "5432", "Dagster")
    conn = engine.raw_connection()
    cursor = conn.cursor()

    df = pd.read_sql_query(f'select * from public."{original_tbl}"', conn)
    df['domain_of_url'] = df['url']
    df['domain_of_url'] = df['domain_of_url'].apply((lambda x: urlparse(x).netloc))

    df.to_sql(new_table, engine, if_exists='replace')
    sql1 = f'select * from public."{new_table}"'
    cursor.execute(sql1)
    for i in cursor.fetchall():
        print(i)

    # метаданные для отображения таблицы в пользовательском интерфейсе Dagster

# table - таблица, из которой пользователь хочет вывести данные в файл, file - название файла

def copy_to_file():
    table = 'Test1'
    file = 'data'
    engine = get_postgres_creds('postgres', 'QWERTY345p', "127.0.0.1", "5432", "Dagster")
    conn = engine.raw_connection()
    cursor = conn.cursor()

    sql1 = f'select * from public."{table}"'
    cursor.execute(sql1)

    with open(file, 'w') as f:
        writer = csv.writer(f)
        for row in cursor.fetchall():
            writer.writerow(row)


copy_to_file()
def main(host, database, user, password):
    load_to_db()
    sql_create_table_as()
    copy_to_file()

if __name__ == '__main__':
    main()
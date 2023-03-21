from .PostrgresDatabase import PostgresDB


DB = PostgresDB()


class BaseTask:
    """Base Pipeline Task"""

    def run(self):
        raise RuntimeError('Do not run BaseTask!')

    def short_description(self):
        pass

    def __str__(self):
        task_type = self.__class__.__name__
        return f'{task_type}: {self.short_description()}'


class CopyToFile(BaseTask):
    """Copy table norm to CSV file"""

    def __init__(self, table, output_file):
        self.table = table

        # добавим папку, в которую сохраняются результаты
        if output_file.rsplit('.', 1)[-1] == "csv":
            self.output_file = output_file
        else:
            self.output_file = f"{output_file}.csv"

    def short_description(self):
        return f'{self.table} -> {self.output_file}'

    def run(self):
        DB.copy_to_file(self.table, self.output_file)
        print(f"Copy table `{self.table}` to file `{self.output_file}`")


class LoadFile(BaseTask):
    """Load file to table"""

    def __init__(self, table, input_file):
        self.table = table
        self.input_file = input_file

    def short_description(self):
        return f'{self.input_file} -> {self.table}'

    def run(self):
        DB.run_query(f"CREATE TABLE IF NOT EXISTS {self.table} (id SERIAL PRIMARY KEY, name varchar, url varchar)")
        DB.load_data_to_table(self.input_file, self.table)

        print(f"Load file `{self.input_file}` to table `{self.table}`")


class RunSQL(BaseTask):
    """Run custom SQL query"""

    def __init__(self, sql_query, title=None):
        self.title = title
        self.sql_query = sql_query

    def short_description(self):
        return f'{self.title}'

    def run(self):
        DB.run_query(self.sql_query)

        print(f"Run SQL ({self.title}):\n{self.sql_query}")


class CTAS(BaseTask):
    """SQL Create Table As Task"""

    def __init__(self, table, sql_query, title=None):
        self.table = table
        self.sql_query = sql_query
        self.title = title or table

    def short_description(self):
        return f'{self.title}'

    def run(self):
        DB.create_table_domain_of_url()
        DB.run_query(f""" CREATE TABLE IF NOT EXISTS {self.table} as {self.sql_query}""")
        print(f"Create table `{self.table}` as SELECT:\n{self.sql_query}")
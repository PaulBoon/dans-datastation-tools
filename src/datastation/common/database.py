from typing import Union
import psycopg
from psycopg.rows import TupleRow


class Database:
    def __init__(self, config):
        self.host = config["host"]
        self.dbname = config["dbname"]
        self.user = config["user"]
        self.password = config["password"]
        self.connection: psycopg.Connection[TupleRow] | None = None

    def connect(self):
        self.connection = psycopg.connect(
            f"host={self.host} dbname={self.dbname} user={self.user} password={self.password}"
        )

    def query(self, query):
        if self.connection is None:
            raise Exception("No connection to database")

        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def update(self, query, *args):
        if self.connection is None:
            raise Exception("No connection to database")

        with self.connection.cursor() as cursor:
            result = cursor.execute(query, *args)
            self.connection.commit()
            return result.rowcount

    def close(self):
        if self.connection is not None:
            self.connection.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

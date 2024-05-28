import sys
from datetime import datetime
from pathlib import Path
from typing import Tuple, List

import numpy as np
import pymssql

from app import App
from temperature import Temperature
from utils import convert_temperature_bytes

project_root_dir_path = Path(__file__).parent
sys.path.append(project_root_dir_path.__str__())

database_host = App.get('database_host')
database_name = App.get('database_name')
database_user = App.get('database_user')
database_password = App.get('database_password')


class Database:
    @staticmethod
    def create_db_connection():
        c = pymssql.connect(host=f'{database_host}', user=database_user,
                            password=database_password,
                            database=database_name)

        return c

    @staticmethod
    def read_circuits(connection: pymssql._pymssql.Connection) -> List[str]:
        # query = "SELECT TOP 3 * FROM BT1110 WHERE CHNCDE = 1 ORDER BY OPTSTR ASC"
        query = "SELECT DISTINCT STRDST FROM CT1020 WHERE STRDST = 0"
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return [row[0] for row in rows]

    @staticmethod
    def read_ct1020_data(connection: pymssql._pymssql.Connection, circuit_name, start_time, end_time) -> Tuple[
        str, List[Temperature]]:
        query = Database.get_reading_temperature_query('CT1020', circuit_name, start_time, end_time)
        with (connection.cursor() as cursor):
            cursor.execute(query)
            rows = cursor.fetchall()
            group_of_temperature = [Temperature(datetime.fromisoformat(row[0]), convert_temperature_bytes(row[3])) for
                                    row in rows]
            return circuit_name, group_of_temperature

    @staticmethod
    def read_ct1022_data(connection: pymssql._pymssql.Connection, circuit_name, start_time, end_time) -> Tuple[
        str, List[Temperature]]:
        query = Database.get_reading_temperature_query('CT1022', circuit_name, start_time, end_time)
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            group_of_temperature = [Temperature(datetime.fromisoformat(row[0]), convert_temperature_bytes(row[3])) for
                                    row in rows]
            return circuit_name, group_of_temperature

    @staticmethod
    def read_ct1010_data(connection: pymssql._pymssql.Connection, circuit_name, start_time, end_time) -> List[
        Temperature]:
        query = f"SELECT * FROM CT1020 WHERE STRDST = '{circuit_name}' AND DATTME > '{start_time}' ORDER BY DATTME"
        with connection.cursor() as cursor:
            cursor.execute(query)
            return [Temperature(datetime.fromisoformat(row[0]), convert_temperature_bytes(row[3])) for row in
                    cursor.fetchall()]

    @staticmethod
    def get_reading_temperature_query(table_name, circuit_name, start_time, end_time):
        return f"""
		SELECT * FROM {table_name} WHERE CIRCDE = '{circuit_name}' AND DATTME BETWEEN '{start_time}'
    	AND '{end_time}' ORDER BY DATTME
		"""

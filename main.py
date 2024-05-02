# import pymssql
#
# from app import App
#
# if __name__ == '__main__':
#     database_host = App.get("DATABASE_HOST")
#     database_user = App.get("DATABASE_USER")
#     database_passwd = App.get("DATABASE_PASSWD")
#     database_name = App.get("DATABASE_NAME")
#     conn = pymssql.connect(host=database_host, user=database_user, password=database_passwd, database=database_name)
#     query = "SELECT * FROM BT1110 WHERE CHNCDE = 1 ORDER BY OPTSTR ASC"
#     with conn.cursor() as cursor:
#         cursor.execute(query)
#         rows = cursor.fetchall()
#         for row in rows:
#             print(row[0], row[8], row[9])


# import datetime
# import sys
# from pathlib import Path
# from typing import List
# from dateutil.relativedelta import relativedelta
#
# import pandas as pd
#
# from temperature import Temperature
# from temperature_service import Database
#
# project_root_dir_path = r'C:\Users\MSI\project\db_connect'
# sys.path.append(project_root_dir_path.__str__())
#
# def convert_to_df(group_of_temperature: List[Temperature]):
#     times = [i.date_time for i in group_of_temperature]
#     temperatures = [i.temperature for i in group_of_temperature]
#     df = pd.DataFrame({'date_time': times})
#     for i, values in enumerate(zip(*temperatures), start=1):
#         df[f'{i}'] = values
#     return df
#
# if __name__ == '__main__':
#     circuit_name = 'test'
#     target_start_date = '2024-04-09 13:25:20'
#     target_end_date = '2024-04-09 13:37:00'
#     con = Database.create_db_connection()
#     group_of_circuit_code = Database.read_circuits(con)
#     current_date = datetime.datetime(2022, 11, 24, 0, 0, 0)
#     global_counter = 1
#
#     for circuit_code in group_of_circuit_code:
#         i = 0
#         if str.find(circuit_code, 'B') == -1:
#             print(f'{circuit_code} is not a circuit code')
#             continue
#         if int(circuit_code[1:3]) < 0:
#             print(f'{circuit_code} is not a circuit code')
#             continue
#
#         while True:
#             next_date = current_date + relativedelta(months=i - 1)
#             next_date_end = next_date + relativedelta(months=1)
#             t = Database.read_ct1020_data(con, circuit_code, next_date, next_date_end)
#             if next_date > datetime.datetime(2024, 1, 1):
#                 break
#             if len(t[1]) == 0:
#                 print(f'{circuit_code}_{next_date} is empty')
#                 i += 1
#                 continue
#
#             df = convert_to_df(t[1])
#             formatted_date = next_date.strftime('%Y-%m-%d')
#             file_name = f'{str(global_counter).zfill(4)}_{circuit_code}_{formatted_date}.csv'
#             df.to_csv(f'{project_root_dir_path}/data/B/{file_name}', columns=df.columns[1:], index=False)
#             print(f'{file_name} is saved')
#             global_counter += 1  # 해당 회로 코드의 파일 카운터를 계속 1 증가
#             i += 1

import datetime
import sys
from pathlib import Path
from typing import List
from dateutil.relativedelta import relativedelta

import pandas as pd

from temperature import Temperature
from temperature_service import Database

# 프로젝트 루트 디렉터리 경로를 설정합니다.
project_root_dir_path = r'C:\Users\MSI\project\db_connect'
sys.path.append(project_root_dir_path.__str__())


def convert_to_df(group_of_temperature: List[Temperature]):
    times = [i.date_time for i in group_of_temperature]
    position_list = [i.temperature for i in group_of_temperature]
    position_df_list = [pd.DataFrame(position, columns=[f'{j}']) for j, position in enumerate(zip(*position_list))]

    position_df = pd.concat(position_df_list, axis=1)
    return position_df

# def convert_to_df(group_of_temperature: List[Temperature]):
#     # 온도 데이터를 DataFrame으로 변환하는 함수입니다.
#     times = [i.date_time for i in group_of_temperature]
#     temperatures = [i.temperature for i in group_of_temperature]
#     df = pd.DataFrame({'date_time': times})
#     for i, values in enumerate(zip(*temperatures), start=1):
#         df[f'temp_{i}'] = values
#     return df



if __name__ == '__main__':
    con = Database.create_db_connection()
    group_of_circuit_code = Database.read_circuits(con)
    current_date = datetime.datetime(2023, 6, 1, 0, 0, 0)

    all_data_frames = []

    for circuit_code in group_of_circuit_code:
        temp_data_frames = []

        i = 0
        while True:
            next_date = current_date + relativedelta(months=i)
            next_date_end = next_date + relativedelta(months=1)
            t = Database.read_ct1020_data(con, circuit_code, next_date, next_date_end)

            if next_date > datetime.datetime(2023, 9, 1):
                break
            if len(t[1]) == 0:
                print(f'{circuit_code}_{next_date} is empty')
                i += 1
                continue

            df = convert_to_df(t[1])
            temp_data_frames.append(df)
            i += 1

        if temp_data_frames:
            circuit_df = pd.concat(temp_data_frames, ignore_index=True)
            all_data_frames.append(circuit_df)

    if all_data_frames:
        combined_df = pd.concat(all_data_frames, axis=1)
        combined_filename = 'combined_data_summer.csv'
        combined_df.to_csv(f'{project_root_dir_path}/data/combine/{combined_filename}', index=False)
        print(f'All data saved in {combined_filename}')


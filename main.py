# all data 추출

import datetime
import sys
from pathlib import Path
from typing import List
from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy as np

from temperature import Temperature
from temperature_service import Database

# 프로젝트 루트 디렉터리 경로를 설정합니다.
project_root_dir_path = r'C:\Users\MSI\Desktop\회사\개발\MLTA\DB_Connect'
sys.path.append(project_root_dir_path.__str__())


def convert_to_df(group_of_temperature: List[Temperature]) -> pd.DataFrame:
    times = [i.date_time for i in group_of_temperature]
    temperatures = [i.temperature for i in group_of_temperature]

    # 최대 길이를 구합니다.
    max_length = max(len(temp) for temp in temperatures)

    # 각 온도 데이터를 동일한 길이로 맞추기 위해 NaN으로 채웁니다.
    padded_temperatures = [np.pad(temp, (0, max_length - len(temp)), 'constant', constant_values=np.nan) for temp in
                           temperatures]

    df = pd.DataFrame({'date_time': times})
    temp_dfs = [pd.DataFrame({f'temp_{i + 1}': col}) for i, col in enumerate(zip(*padded_temperatures))]
    temp_df = pd.concat(temp_dfs, axis=1)
    return pd.concat([df, temp_df], axis=1)


def process_time_range(con, circuit_code, start_time, end_time):
    temp_data_frames = []
    next_date = start_time
    while next_date < end_time:
        next_date_end = next_date + relativedelta(months=1)
        t = Database.read_ct1020_data(con, circuit_code, next_date, next_date_end)
        if not t:
            print(f'{circuit_code}_{next_date} is empty')
            next_date += relativedelta(months=1)
            continue

        df = convert_to_df(t)
        temp_data_frames.append(df)
        next_date += relativedelta(months=1)

    if temp_data_frames:
        return pd.concat(temp_data_frames, ignore_index=True)
    return pd.DataFrame()


if __name__ == '__main__':
    con = Database.create_db_connection()
    group_of_circuit_code = Database.read_circuits(con)
    current_date = datetime.datetime(2024, 6, 14, 12, 30, 0)

    all_data_frames = []

    for circuit_code in group_of_circuit_code:
        circuit_df = process_time_range(con, circuit_code, current_date, datetime.datetime(2024, 8, 1))
        if not circuit_df.empty:
            all_data_frames.append(circuit_df)

    if all_data_frames:
        combined_df = pd.concat(all_data_frames, axis=1)
        combined_filename = '5_hour.csv'
        combined_df.to_csv(f'{project_root_dir_path}/data/GS_ENR_0.25/{combined_filename}', index=False)
        print(f'All data saved in {combined_filename}')



# 0.25 기준 3천 미터 추출
# import datetime
# import sys
# from pathlib import Path
# from typing import List
# from dateutil.relativedelta import relativedelta
#
# import pandas as pd
# import numpy as np
#
# from temperature import Temperature
# from temperature_service import Database
#
# # 프로젝트 루트 디렉터리 경로를 설정합니다.
# project_root_dir_path = r'C:\Users\MSI\project\db_connect'
# sys.path.append(project_root_dir_path.__str__())
#
#
# def convert_to_df(group_of_temperature: List[Temperature], max_length: int = 12000) -> pd.DataFrame:
#     times = [i.date_time for i in group_of_temperature]
#     temperatures = [i.temperature[:max_length] for i in group_of_temperature]
#
#     # 최대 길이를 12,000으로 제한하고 부족한 부분을 NaN으로 채웁니다.
#     padded_temperatures = [np.pad(temp, (0, max_length - len(temp)), 'constant', constant_values=np.nan) for temp in
#                            temperatures]
#
#     df = pd.DataFrame({'date_time': times})
#     temp_dfs = [pd.DataFrame({f'temp_{i + 1}': col}) for i, col in enumerate(zip(*padded_temperatures))]
#     temp_df = pd.concat(temp_dfs, axis=1)
#     return pd.concat([df, temp_df], axis=1)
#
#
# def process_time_range(con, circuit_code, start_time, end_time):
#     temp_data_frames = []
#     next_date = start_time
#     while next_date < end_time:
#         next_date_end = next_date + relativedelta(months=1)
#         t = Database.read_ct1020_data(con, circuit_code, next_date, next_date_end)
#         if not t:
#             print(f'{circuit_code}_{next_date} is empty')
#             next_date += relativedelta(months=1)
#             continue
#
#         df = convert_to_df(t)
#         temp_data_frames.append(df)
#         next_date += relativedelta(months=1)
#
#     if temp_data_frames:
#         return pd.concat(temp_data_frames, ignore_index=True)
#     return pd.DataFrame()
#
#
# if __name__ == '__main__':
#     con = Database.create_db_connection()
#     group_of_circuit_code = Database.read_circuits(con)
#     current_date = datetime.datetime(2024, 6, 5, 17, 15, 0)
#
#     all_data_frames = []
#
#     for circuit_code in group_of_circuit_code:
#         circuit_df = process_time_range(con, circuit_code, current_date, datetime.datetime(2024, 7, 1))
#         if not circuit_df.empty:
#             all_data_frames.append(circuit_df)
#
#     if all_data_frames:
#         combined_df = pd.concat(all_data_frames, axis=1)
#         combined_filename = 'data.csv'
#         combined_df.to_csv(f'{project_root_dir_path}/data/24_06_10/{combined_filename}', index=False)
#         print(f'All data saved in {combined_filename}')

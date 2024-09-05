# all data 추출

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
# project_root_dir_path = r'C:\Users\MSI\Desktop\DTS#2'
# sys.path.append(project_root_dir_path.__str__())
#
#
# def convert_to_df(group_of_temperature: List[Temperature]) -> pd.DataFrame:
#     times = [i.date_time for i in group_of_temperature]
#     temperatures = [i.temperature for i in group_of_temperature]
#
#     # 최대 길이를 구합니다.
#     max_length = max(len(temp) for temp in temperatures)
#
#     # 각 온도 데이터를 동일한 길이로 맞추기 위해 NaN으로 채웁니다.
#     padded_temperatures = [np.pad(temp, (0, max_length - len(temp)), 'constant', constant_values=np.nan) for temp in
#                            temperatures]
#
#     df = pd.DataFrame({'date_time': times})
#     temp_dfs = [pd.DataFrame({f'{i / 4}': col}) for i, col in enumerate(zip(*padded_temperatures))]
#     temp_df = pd.concat(temp_dfs, axis=1)
#     return pd.concat([df, temp_df], axis=1)
#
#
# def process_time_range(con, circuit_code, start_time, end_time):
#     temp_data_frames = []
#     next_date = start_time
#     while next_date < end_time:
#         next_date_end = next_date + relativedelta(days=1)
#         t = Database.read_ct1020_data(con, circuit_code, next_date, next_date_end)
#         if not t:
#             print(f'{circuit_code}_{next_date} is empty')
#             next_date += relativedelta(days=1)
#             continue
#
#         df = convert_to_df(t)
#         temp_data_frames.append(df)
#         next_date += relativedelta(days=1)
#
#     if temp_data_frames:
#         return pd.concat(temp_data_frames, ignore_index=True)
#     return pd.DataFrame()
#
#
# if __name__ == '__main__':
#     con = Database.create_db_connection()
#     group_of_circuit_code = Database.read_circuits(con)
#     current_date = datetime.datetime(2024, 6, 1, 00, 0, 0)
#
#     all_data_frames = []
#
#     for circuit_code in group_of_circuit_code:
#         circuit_df = process_time_range(con, circuit_code, current_date, datetime.datetime(2024, 6, 2))
#         if not circuit_df.empty:
#             all_data_frames.append(circuit_df)
#
#     if all_data_frames:
#         combined_df = pd.concat(all_data_frames, axis=1)
#         combined_filename = 'DTS#2_2.csv'
#         combined_df.to_csv(f'{project_root_dir_path}/{combined_filename}', index=False)
#         print(f'All data saved in {combined_filename}')



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
#         next_date_end = next_date + relativedelta(days=1)
#         t = Database.read_ct1020_data(con, circuit_code, next_date, next_date_end)
#         if not t:
#             print(f'{circuit_code}_{next_date} is empty')
#             next_date += relativedelta(days=1)
#             continue
#
#         df = convert_to_df(t)
#         temp_data_frames.append(df)
#         next_date += relativedelta(days=1)
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

import datetime
import sys
from pathlib import Path
from typing import List
from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy as np
from loguru import logger

from temperature import Temperature
from temperature_service import Database

# 프로젝트 루트 디렉터리 경로를 설정합니다.
project_root_dir_path = r'C:\Users\MSI\Desktop\DTS#2'
sys.path.append(project_root_dir_path.__str__())

# 로그 설정
log_file_path = Path(project_root_dir_path) / 'process.log'
logger.add(log_file_path, rotation="1 MB", retention="10 days", level="DEBUG")


def convert_to_df(group_of_temperature: List[Temperature]) -> pd.DataFrame:
    logger.debug("Converting temperature group to DataFrame")
    times = [i.date_time for i in group_of_temperature]
    temperatures = [i.temperature for i in group_of_temperature]

    # 최대 길이를 구합니다.
    max_length = max(len(temp) for temp in temperatures)
    logger.debug(f"Max length of temperature lists: {max_length}")

    # 각 온도 데이터를 동일한 길이로 맞추기 위해 NaN으로 채웁니다.
    padded_temperatures = [np.pad(temp, (0, max_length - len(temp)), 'constant', constant_values=np.nan) for temp in
                           temperatures]

    df = pd.DataFrame({'date_time': times})
    temp_dfs = [pd.DataFrame({f'{i / 4}': col}) for i, col in enumerate(zip(*padded_temperatures))]
    temp_df = pd.concat(temp_dfs, axis=1)
    logger.debug("Temperature data conversion complete")
    return pd.concat([df, temp_df], axis=1)


def process_time_range(con, circuit_code, date):
    logger.info(f"Processing data for circuit {circuit_code} on {date}")
    next_date = date + relativedelta(days=1)

    logger.debug(f"Fetching data from {date} to {next_date} for circuit {circuit_code}")
    t = Database.read_ct1020_data(con, circuit_code, date, next_date)

    if not t:
        logger.warning(f"No data found for circuit {circuit_code} on {date}")
        return None

    df = convert_to_df(t)
    logger.debug(f"Data frame created for circuit {circuit_code} on {date}")
    return df


if __name__ == '__main__':
    logger.info("Starting the temperature processing script")
    con = Database.create_db_connection()
    logger.debug("Database connection established")

    group_of_circuit_code = Database.read_circuits(con)
    logger.debug(f"Circuits retrieved: {group_of_circuit_code}")

    start_date = datetime.datetime(2024, 5, 24, 0, 0, 0)
    end_date = datetime.datetime(2024, 9, 4, 0, 0, 0)
    current_date = start_date

    while current_date < end_date:
        for circuit_code in group_of_circuit_code:
            logger.info(f"Processing circuit {circuit_code} on {current_date}")
            circuit_df = process_time_range(con, circuit_code, current_date)
            if circuit_df is not None:
                filename = f'DTS#2_{current_date.strftime("%Y%m%d")}.csv'
                circuit_df.to_csv(Path(project_root_dir_path) / filename, index=False)
                logger.info(f"Data for {circuit_code} on {current_date} saved in {filename}")
            else:
                logger.info(f"No data found for {circuit_code} on {current_date}")

        current_date += relativedelta(days=1)

    logger.info("Temperature processing script completed")


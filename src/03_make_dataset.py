"""
***********************************************************************************************************************
    imports
***********************************************************************************************************************
"""

import time
import numpy as np
import pandas as pd
from os import listdir
from os.path import isfile, join
from typing import List
import datetime
import json

"""
***********************************************************************************************************************
    DataSetMaker Class
***********************************************************************************************************************
"""


class DataSetMaker:
    """
    Class that can make a TimeSeriesDataSet from merged files.
    """
    def __init__(self, source_path, destination_path):
        self.source_path = source_path
        self.destination_path = destination_path

    """
    *******************************************************************************************************************
        Helper functions - static
    *******************************************************************************************************************
    """

    @staticmethod
    def __is_not_time_stamp(some_string_or_timestamp):
        if isinstance(some_string_or_timestamp, str):
            assert isinstance(some_string_or_timestamp, str)
            has_digit = any(char.isdigit() for char in some_string_or_timestamp)
            has_line = some_string_or_timestamp.count("-") > 0
            return not has_digit and not has_line
        else:
            return False

    @staticmethod
    def __get_first_index_that_is_not_time_stamp(columns: List[str]) -> int:
        first_index_that_is_date_time = 0
        for i in range(len(columns)):
            if DataSetMaker.__is_not_time_stamp(columns[i]):
                pass
            else:
                "is time stamp"
                assert first_index_that_is_date_time == 0
                first_index_that_is_date_time = i
                break
        return first_index_that_is_date_time

    @staticmethod
    def __read_data_frame(csv_path):
        # read dataframe from csv
        print(f"Reading dataframe.")
        print(f"csv_path = '{csv_path}'")
        _start = time.time()
        df = pd.read_csv(
            filepath_or_buffer=csv_path,
            index_col=0,
        )
        _end = time.time()
        print(f"Reading took {_end - _start} seconds")

        first_dt = DataSetMaker.__get_first_index_that_is_not_time_stamp(df.columns)
        height_of_df = df.shape[0]
        width_of_df = df.shape[1]

        "turn date time columns into datetime format"
        df.columns = list(df.columns[:first_dt]) + list(
            (pd.to_datetime(df.columns[first_dt:], format='%Y-%m-%d_%H_%M_%S')))

        "drop columns that are entirely empty"
        df.dropna(axis=1, how='all', inplace=True)

        "drop rows that are ALMOST entirely empty"
        df.dropna(axis=0, thresh=(width_of_df // 100), inplace=True)

        # "cast to numeric"
        # df = df.astype(dtype='float64')

        return df

    @staticmethod
    def __get_names_of_files_in_directory_sorted(directory_path):
        csv_names = [f for f in listdir(directory_path) if (isfile(join(directory_path, f)) and ("txt" not in f))]
        csv_names.sort()
        return csv_names

    @staticmethod
    def __are_we_about_to_skip_a_minute(j: int, row_of_df):
        assert (0 <= j) and (j < len(row_of_df))
        if (j + 1) == len(row_of_df):
            return True
        else:
            ind = row_of_df.index
            ind_j = ind[j]
            ind_jp1 = ind[j + 1]
            diff = ind_jp1 - ind_j
            return diff > datetime.timedelta(minutes=1)

    @staticmethod
    def __extract_time_series_list_from_range(row_of_df, last_time_series_index, next_time_series_index):
        "take the time series we just saw out"
        uninterrupted_time_series_as_pandas_series = row_of_df[last_time_series_index: next_time_series_index]

        "turn time series into list of tuples of timestamp and value"
        uninterrupted_time_series_as_df = pd.DataFrame(uninterrupted_time_series_as_pandas_series)
        uninterrupted_time_series_as_iter_tuples = uninterrupted_time_series_as_df.itertuples(
            index=False, name=None
        )
        uninterrupted_time_series_as_list = [x[0] for x in uninterrupted_time_series_as_iter_tuples]
        start_time = row_of_df.index[last_time_series_index]
        stop_time = row_of_df.index[next_time_series_index - 1]
        time_delta_in_minutes = (stop_time - start_time).total_seconds() / 60
        assert (time_delta_in_minutes + 1) == len(uninterrupted_time_series_as_list)
        return {
            "start": start_time,
            "stop": stop_time,
            "data": uninterrupted_time_series_as_list
        }

    @staticmethod
    def __get_key_and_list_of_time_series_for_row_in_df(row_of_df, first_index_not_time_stamp):
        result = []

        "drop rows that are entirely empty"
        row_of_df = row_of_df.dropna()

        "figure out what they key of the dictionary should be"
        key_of_dict = ', '.join(row_of_df[:first_index_not_time_stamp])

        number_of_columns = len(row_of_df)
        last_time_series_index = first_index_not_time_stamp

        for j in range(first_index_not_time_stamp, number_of_columns):
            if DataSetMaker.__are_we_about_to_skip_a_minute(j=j, row_of_df=row_of_df):
                current_item = DataSetMaker.__extract_time_series_list_from_range(
                    row_of_df=row_of_df,
                    last_time_series_index=last_time_series_index,
                    next_time_series_index=j + 1
                )
                result.append(current_item)
                last_time_series_index = j + 1
        return key_of_dict, result

    @staticmethod
    def __split_df(df):
        result = {}
        number_of_rows = df.shape[0]
        fi = DataSetMaker.__get_first_index_that_is_not_time_stamp(df.columns)
        r = []

        print("Splitting data:")
        for i in range(number_of_rows):
            print(f"Iteration number {i + 1} / {number_of_rows}")
            k, v = DataSetMaker.__get_key_and_list_of_time_series_for_row_in_df(
                row_of_df=df.iloc[i],
                first_index_not_time_stamp=fi
            )
            assert k not in result.keys()
            result[k] = v
            r += v
        assert len(result) == number_of_rows
        return result

    """
    *******************************************************************************************************************
        Helper functions - non static
    *******************************************************************************************************************
    """

    def __save_json_result(self, result, csv_file):
        class PdEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, pd.Timestamp):
                    return str(obj)
                return json.JSONEncoder.default(self, obj)

        _start = time.time()
        file_path = f"{self.destination_path}{csv_file[:-4]}.json"
        print(f"Writing json to '{file_path}'")
        with open(file_path, "w") as a_file:
            # print("Saving with indent = 1 !")
            json.dump(result, a_file, indent=1, cls=PdEncoder)
        _end = time.time()
        print(f"Reading took {_end - _start} seconds")
        return file_path

    def __make_data_set_and_save_it(self, csv_file):
        df = self.__read_data_frame(csv_path=f"{self.source_path}{csv_file}")
        time_series = self.__split_df(df=df)
        file_path = self.__save_json_result(result=time_series, csv_file=csv_file)

    """
    *******************************************************************************************************************
        API functions
    *******************************************************************************************************************
    """

    def make_data_sets_and_save_them(self):
        list_of_files = self.__get_names_of_files_in_directory_sorted(self.source_path)
        for csv_file in list_of_files:
            self.__make_data_set_and_save_it(csv_file=csv_file)


"""
***********************************************************************************************************************
    main function
***********************************************************************************************************************
"""


def main():
    print("""
    This script takes a merged data file from the previous step
    and makes a dataset out of that data. This data can then be 
    used with deep learning models.
    
    The scripts takes data from data/step_3__data_sets/CSVs_to_turn_to_datasets/
    and writes the datasets into data/step_3__data_sets/datasets/
    One dataset will be generated for each file.
    """)

    dsm = DataSetMaker(
        source_path="../data/step_3__data_sets/CSVs_to_turn_to_datasets/",
        destination_path="../data/step_3__data_sets/datasets/"
    )
    dsm.make_data_sets_and_save_them()
    print("Done!")


"""
***********************************************************************************************************************
    run main function
***********************************************************************************************************************
"""

if __name__ == "__main__":
    main()

"""
***********************************************************************************************************************
    imports
***********************************************************************************************************************
"""

import pandas
from os import listdir
from os.path import isfile, join
import time

"""
***********************************************************************************************************************
    Data Merger Class
***********************************************************************************************************************
"""


class DataMerger:
    """
    Class that can merge fetched data from prometheus in operate first.
    """
    def __init__(self, path_to_data):
        self.path_to_data = path_to_data
        self.csv_names = [f for f in listdir(self.path_to_data) if isfile(join(self.path_to_data, f))]
        self.csv_names.sort()
        self.current_time_segment_data_frame = None
        self.last_save_iteration = 0

    """
    *******************************************************************************************************************
        Helper functions
    *******************************************************************************************************************
    """

    @staticmethod
    def __is_not_time_stamp(some_string: str):
        has_digit = any(char.isdigit() for char in some_string)
        has_line = some_string.count("-") > 0
        return not has_digit and not has_line

    @staticmethod
    def __get_cols_to_merge_on(df_1, df_2):
        cols1 = list(df_1.columns)
        cols2 = list(df_2.columns)
        cols_intersection = list(set(cols1).intersection(cols2))
        filtered_cols_intersection = list(filter(DataMerger.__is_not_time_stamp, cols_intersection))
        return filtered_cols_intersection

    def __add_given_dataframe_to_output_dataframe(self, dataframe_to_add):
        if self.current_time_segment_data_frame is None:
            self.current_time_segment_data_frame = dataframe_to_add
        else:
            start = time.time()
            merged_df = self.current_time_segment_data_frame.merge(
                right=dataframe_to_add,
                how="outer",
                sort=True,
                on=self.__get_cols_to_merge_on(df_1=self.current_time_segment_data_frame, df_2=dataframe_to_add)
            )
            end = time.time()
            print("merging took ", end - start)
            # if end - start > 0.5:
            #     print("Taking too much time")
            self.current_time_segment_data_frame = merged_df

    def __did_we_just_miss_an_hour(self, index):
        if index == 0:
            return False
        else:
            ending_hour_of_last_index = self.csv_names[index - 1][-23:-4]
            starting_hour_of_current_index = self.csv_names[index][:19]
            return ending_hour_of_last_index != starting_hour_of_current_index

    def __save_and_free_current_time_segment(self, last_save_iteration, current_iteration):
        starting_hour_of_last_save_iteration = self.csv_names[last_save_iteration][:19]
        ending_hour_of_current_iteration = self.csv_names[current_iteration][-23:-4]
        elapsed_hours = current_iteration - last_save_iteration + 1
        path = f"{self.path_to_data}_{starting_hour_of_last_save_iteration}_to_{ending_hour_of_current_iteration}__{elapsed_hours}_hours.csv"
        print("Save to path = ", path)
        start = time.time()
        self.current_time_segment_data_frame.to_csv(
            path_or_buf=path,
            index=False
        )
        end = time.time()
        print("writing took ", end - start)
        self.current_time_segment_data_frame = None

    def __check_if_current_time_segment_ended_and_save_if_yes(self, index):
        if self.__did_we_just_miss_an_hour(index=index):
            self.__save_and_free_current_time_segment(
                last_save_iteration=self.last_save_iteration,
                current_iteration=index-1
            )
            self.last_save_iteration = index

    """
    *******************************************************************************************************************
        API functions
    *******************************************************************************************************************
    """

    def merge_data(self):
        self.last_save_iteration = 0
        print("number of files = ", len(self.csv_names))
        for i, csv_name in enumerate(self.csv_names):
            self.__check_if_current_time_segment_ended_and_save_if_yes(index=i)
            print("current file = ", (i + 1), " / ", len(self.csv_names))
            current_dataframe = pandas.read_csv(
                filepath_or_buffer=f"{self.path_to_data}/{csv_name}",
                index_col=0
            )
            self.__add_given_dataframe_to_output_dataframe(dataframe_to_add=current_dataframe)

        print("Saving final dataframe")
        self.__save_and_free_current_time_segment(
            last_save_iteration=self.last_save_iteration,
            current_iteration=len(self.csv_names) - 1
        )


"""
***********************************************************************************************************************
    main function
***********************************************************************************************************************
"""


def main():
    print("""
    Thank you for using our tool. 
    This tool merges the data in 
    ../data/container_cpu_usage_seconds
    ../data/container_memory_working_set_bytes
    ../data/node_memory_active_bytes_percentage
    """)

    container_cpu_merger = DataMerger("../data/container_cpu_usage_seconds")
    container_mem_merger = DataMerger("../data/container_memory_working_set_bytes")
    node_mem_merger = DataMerger("../data/node_memory_active_bytes_percentage")

    container_cpu_merger.merge_data()
    container_mem_merger.merge_data()
    node_mem_merger.merge_data()


"""
***********************************************************************************************************************
    run main function
***********************************************************************************************************************
"""

if __name__ == "__main__":
    main()

"""
***********************************************************************************************************************
    imports
***********************************************************************************************************************
"""

import pandas
from os import listdir
from os.path import isfile, join
import time
import os

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
        self.folder_path = "../data/step_2__data_islands/"

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

    def __get_merger_of_two_data_frames(self, left_df, right_df):
        # start = time.time()
        merged_df = left_df.merge(
            right=right_df,
            how="outer",
            sort=True,
            on=self.__get_cols_to_merge_on(df_1=left_df, df_2=right_df)
        )
        # end = time.time()
        # print("merging took ", end - start)
        return merged_df

    def __add_given_dataframe_to_output_dataframe(self, dataframe_to_add):
        if self.current_time_segment_data_frame is None:
            self.current_time_segment_data_frame = dataframe_to_add
        else:
            "drop last minute since the new df also has that sample"
            current_with_dropped_last_col = self.current_time_segment_data_frame.iloc[:, :-1]
            "merge the two data frames"
            merged_df = self.__get_merger_of_two_data_frames(
                left_df=current_with_dropped_last_col,
                right_df=dataframe_to_add
            )
            "replace the saved dataframe"
            self.current_time_segment_data_frame = merged_df

    def __did_we_just_miss_an_hour(self, index):
        if index == 0:
            return False
        else:
            ending_hour_of_last_index = self.csv_names[index - 1][-23:-4]
            starting_hour_of_current_index = self.csv_names[index][:19]
            return ending_hour_of_last_index != starting_hour_of_current_index

    def __save_data_frame(self, path):
        print("Save to path = ", path)
        start = time.time()
        transposed = self.current_time_segment_data_frame.transpose()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        transposed.to_csv(
            path_or_buf=path,
        )
        end = time.time()
        print("writing took ", end - start)

    def __get_path_to_save_data_frame_in(self, last_save_iteration, current_iteration):
        starting_hour_of_last_save_iteration = self.csv_names[last_save_iteration][:19]
        ending_hour_of_current_iteration = self.csv_names[current_iteration][-23:-4]
        elapsed_hours = current_iteration - last_save_iteration + 1
        metric = self.path_to_data.split("/")[-1]
        path = f"{self.folder_path}{metric}/{starting_hour_of_last_save_iteration}_to_{ending_hour_of_current_iteration}__{elapsed_hours}_hours.csv"
        return path

    def __save_and_free_current_time_segment(self, last_save_iteration, current_iteration):
        self.__save_data_frame(
            path=self.__get_path_to_save_data_frame_in(
                last_save_iteration=last_save_iteration,
                current_iteration=current_iteration
            )
        )
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

    container_cpu_merger = DataMerger("../data/step_1__continuous_data_fetching/container_cpu_usage_seconds")
    container_mem_merger = DataMerger("../data/step_1__continuous_data_fetching/container_memory_working_set_bytes")
    node_mem_merger = DataMerger("../data/step_1__continuous_data_fetching/node_memory_active_bytes_percentage")

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

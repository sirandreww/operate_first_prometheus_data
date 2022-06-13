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
    def __init__(self, path_to_data: str):
        self.path_to_data = path_to_data
        self.folder_path = f"../data/step_2__data_islands/{path_to_data.split('/')[-1]}"

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
        start = time.time()
        merged_df = left_df.merge(
            right=right_df,
            how="outer",
            sort=True,
            on=self.__get_cols_to_merge_on(df_1=left_df, df_2=right_df)
        )
        end = time.time()
        print("merging took ", end - start)
        return merged_df

    @staticmethod
    def __save_data_frame(path, data_frame):
        print("Save to path = ", path)
        start = time.time()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        data_frame.to_csv(
            path_or_buf=path,
        )
        end = time.time()
        print("writing took ", end - start)

    @staticmethod
    def __get_path_to_save_data_frame_in(first_csv_name, second_csv_name, destination_directory):
        starting_hour_of_last_save_iteration = first_csv_name[:19]
        ending_hour_of_current_iteration = second_csv_name[-23:-4]
        path = f"{destination_directory}/{starting_hour_of_last_save_iteration}_to_{ending_hour_of_current_iteration}.csv"
        return path

    @staticmethod
    def __load_csv_as_data_frame(path_to_csv: str):
        current_dataframe = pandas.read_csv(
            filepath_or_buffer=path_to_csv,
            index_col=0
        )
        return current_dataframe

    @staticmethod
    def __get_names_of_files_in_directory_sorted(directory_path):
        csv_names = [f for f in listdir(directory_path) if isfile(join(directory_path, f))]
        csv_names.sort()
        return csv_names

    def __merge_two_consecutive_files_and_save_them(self, source_directory, destination_directory, csv_names, index_1):
        index_2 = index_1 + 1
        dataframe_1 = self.__load_csv_as_data_frame(path_to_csv=f"{source_directory}/{csv_names[index_1]}")
        if index_2 == len(csv_names):
            "move data frame to new path"
            self.__save_data_frame(
                path=self.__get_path_to_save_data_frame_in(
                    first_csv_name=csv_names[index_1],
                    second_csv_name=csv_names[index_1],
                    destination_directory=destination_directory
                ),
                data_frame=dataframe_1
            )
        else:
            dataframe_2 = self.__load_csv_as_data_frame(path_to_csv=f"{source_directory}/{csv_names[index_2]}")
            "drop last minute in first data frame since the second data frame also has that sample"
            dataframe_1_without_last_minute = dataframe_1.iloc[:, :-1]
            "merge the two data frames"
            merged_df = self.__get_merger_of_two_data_frames(
                left_df=dataframe_1_without_last_minute,
                right_df=dataframe_2
            )
            "save new merged data frame"
            self.__save_data_frame(
                path=self.__get_path_to_save_data_frame_in(
                    first_csv_name=csv_names[index_1],
                    second_csv_name=csv_names[index_2],
                    destination_directory=destination_directory
                ),
                data_frame=merged_df
            )

    def __perform_merging_iteration(self, csv_names, source_directory, destination_directory):
        print("len(csv_names) = ", len(csv_names))
        number_of_iterations = (len(csv_names) + 1) // 2
        print("number_of_iterations = ", number_of_iterations)
        for i in range(number_of_iterations):
            print("progress = ", i + 1, " / ", number_of_iterations)
            index_1 = 2 * i
            self.__merge_two_consecutive_files_and_save_them(
                source_directory=source_directory,
                destination_directory=destination_directory,
                csv_names=csv_names,
                index_1=index_1
            )

    """
    *******************************************************************************************************************
        API functions
    *******************************************************************************************************************
    """

    def merge_data(self):
        # self.last_save_iteration = 0
        # print("number of files = ", len(self.csv_names))
        merge_iteration = 0
        source_directory = self.path_to_data
        destination_directory = f"{self.folder_path}/iteration_1"
        while True:
            print("current iteration = ", (merge_iteration + 1))
            csv_names = self.__get_names_of_files_in_directory_sorted(directory_path=source_directory)
            assert len(csv_names) != 0
            if len(csv_names) == 1:
                break
            else:
                self.__perform_merging_iteration(
                    csv_names=csv_names,
                    source_directory=source_directory,
                    destination_directory=destination_directory
                )
                source_directory = destination_directory
                destination_directory = f"{self.folder_path}/iteration_{merge_iteration + 2}"
            merge_iteration += 1


        # print("Saving final dataframe")
        # self.__save_and_free_current_time_segment(
        #     last_save_iteration=self.last_save_iteration,
        #     current_iteration=len(self.csv_names) - 1
        # )


"""
***********************************************************************************************************************
    main function
***********************************************************************************************************************
"""


def main():
    print("""
    Thank you for using our tool. 
    This tool merges the data in 
    ../data/step_1__continuous_data_fetching/container_cpu_usage_seconds
    ../data/step_1__continuous_data_fetching/container_memory_working_set_bytes
    ../data/step_1__continuous_data_fetching/node_memory_active_bytes_percentage
    
    This script merges every two files and saves the output. 
    And does this repeatedly until we have one file.
    It is designed to do so in order to reduce memory usage and be able to merge longer files.
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

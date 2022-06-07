"""
***********************************************************************************************************************
    imports
***********************************************************************************************************************
"""

import datetime
from prometheus_api_client import PrometheusConnect, PrometheusApiClientException
from pandas import DataFrame
import os.path

"""
***********************************************************************************************************************
    Data Fetcher Class
***********************************************************************************************************************
"""


class DataFetcher:
    """
    Class that can fetch data from prometheus in operate first.
    """
    def __init__(self, access_token, url_to_fetch_from):
        self.access_token = access_token
        self.url_to_fetch_from = url_to_fetch_from
        self.prometheus_connection = None
        self.metrics = [
            "node_memory_active_bytes_percentage",
            "container_memory_working_set_bytes",
            "container_cpu_usage_seconds"
        ]

    """
    *******************************************************************************************************************
        Helper functions
    *******************************************************************************************************************
    """

    @staticmethod
    def __convert_datetime_to_string(date_time) -> str:
        return str(date_time).replace(":", "_").replace(" ", "_")

    @staticmethod
    def __get_string_from_timestamp(time_stamp):
        def __convert_timestamp_to_datetime(time_stamp):
            return datetime.datetime.fromtimestamp(time_stamp)

        return DataFetcher.__convert_datetime_to_string(__convert_timestamp_to_datetime(time_stamp))

    @staticmethod
    def __prepare_dictionary_to_house_query_result(data, start_time: datetime.datetime, end_time: datetime.datetime,
                                                   step: int):
        assert data is not None
        final_result_as_dict = {}

        #  allocate headers
        metric_dictionary_keys = data[0]['metric'].keys()
        for key in metric_dictionary_keys:
            final_result_as_dict[key] = []

        current_time = start_time
        time_stamps = []
        while current_time <= end_time:
            time_stamps.append(DataFetcher.__convert_datetime_to_string(current_time))
            current_time += datetime.timedelta(seconds=step)
        assert len(time_stamps) == 61

        for key in time_stamps:
            final_result_as_dict[key] = []

        return final_result_as_dict, metric_dictionary_keys, time_stamps

    @staticmethod
    def __insert_data_query_result_into_dictionary(final_result_as_dict, metric_dictionary_keys,
                                                   time_stamps, data):
        length = 0  # this will ne used for checking legality
        # for each container, put the data in the dictionary
        for container in data:
            assert container['metric'].keys() == metric_dictionary_keys
            for key, value in container['metric'].items():
                final_result_as_dict[key].append(value)

            # fill time slots
            for tv in container['values']:
                final_result_as_dict[DataFetcher.__get_string_from_timestamp(tv[0])].append(tv[1])

            # fill remaining time slots wit None
            # go over all the minutes in the hour
            for time in time_stamps:
                # if there is no value for this minute put in None.
                if time not in [DataFetcher.__get_string_from_timestamp(tv[0]) for tv in container['values']]:
                    final_result_as_dict[time].append(None)

            length += 1
            for key, value in final_result_as_dict.items():
                assert length == len(value)
        assert length == len(data)
        assert len(final_result_as_dict) == 61 + len(metric_dictionary_keys)

        return DataFrame(final_result_as_dict)

    @staticmethod
    def __convert_query_result_to_data_frame(data, start_time: datetime.datetime, end_time: datetime.datetime,
                                             step: int) -> DataFrame:
        assert data is not None
        a, b, c = DataFetcher.__prepare_dictionary_to_house_query_result(
            data=data,
            start_time=start_time,
            end_time=end_time,
            step=step
        )
        final_result_as_dict, metric_dictionary_keys, time_stamps = (a, b, c)
        return DataFetcher.__insert_data_query_result_into_dictionary(
            data=data,
            final_result_as_dict=final_result_as_dict,
            metric_dictionary_keys=metric_dictionary_keys,
            time_stamps=time_stamps
        )

    def __convert_metric_to_query(self, metric: str) -> str:
        assert metric in self.metrics
        result = ""
        if metric == "container_memory_working_set_bytes":
            result = 'sum(container_memory_working_set_bytes{name!~".*prometheus.*", image!="", container!="POD", cluster="moc/smaug"}) by (container, pod, namespace, node)'
        elif metric == "container_cpu_usage_seconds":
            result = 'sum(rate(container_cpu_usage_seconds_total{name!~".*prometheus.*", image!="", container!="POD", cluster="moc/smaug"}[5m])) by (container, pod, namespace, node)'
        elif metric == "node_memory_active_bytes_percentage":
            result = 'node_memory_Active_bytes/node_memory_MemTotal_bytes*100'
        else:
            assert False
        return result

    def __get_data_in_certain_range(self, start_time, end_time, query: str, csv_path: str):
        print("getting data for ", start_time, " to ", end_time)
        metric_df = None
        if os.path.exists(csv_path):
            print("Data already exists, moving on!")
        else:
            step = 60  # seconds
            print("running query : ", query)
            metric_data = self.prometheus_connection.custom_query_range(
                query=query,
                start_time=start_time,
                end_time=end_time,
                step=str(step)
            )
            if not metric_data:
                print("Got empty results, moving on!")
            else:
                metric_df = self.__convert_query_result_to_data_frame(
                    data=metric_data,
                    start_time=start_time,
                    end_time=end_time,
                    step=step
                )
                print("saving csv to file : ", csv_path)
                os.makedirs(os.path.dirname(csv_path), exist_ok=True)
                metric_df.to_csv(csv_path)
        return metric_df

    """
    *******************************************************************************************************************
        API functions
    *******************************************************************************************************************
    """

    def create_connection(self):
        self.prometheus_connection = PrometheusConnect(
            url=self.url_to_fetch_from,
            headers={"Authorization": f"Bearer {self.access_token}"},
            disable_ssl=False
        )
        time_now = datetime.datetime.now()
        _end_time = time_now.replace(minute=0, second=0, microsecond=0)
        _start_time = _end_time - datetime.timedelta(hours=1)
        self.prometheus_connection.custom_query_range(
            query="num_of_allocatable_nodes",
            start_time=_start_time,
            end_time=_end_time,
            step=str(60)
        )

    def get_metric_data_for_the_past_number_of_hours(self):
        for metric in self.metrics:
            time_now = datetime.datetime.now()
            _end_time = time_now.replace(minute=0, second=0, microsecond=0)
            _start_time = _end_time - datetime.timedelta(hours=1)
            print("")
            for i in range(24 * 10):  # try 10 days back
                self.__get_data_in_certain_range(
                    start_time=_start_time,
                    end_time=_end_time,
                    query=self.__convert_metric_to_query(metric),
                    csv_path=f'../data/{metric}/{_start_time}_to_{_end_time}.csv'.replace(":", "_").replace(" ", "_")
                )
                _end_time = _start_time
                _start_time = _end_time - datetime.timedelta(hours=1)


"""
***********************************************************************************************************************
    main function
***********************************************************************************************************************
"""


def main():
    print("""
    Thank you for using our tool. Let's get some data.
    You can get an access token here :
    https://console-openshift-console.apps.smaug.na.operate-first.cloud/
    or better yet:
    https://oauth-openshift.apps.smaug.na.operate-first.cloud/oauth/token/request
    """)

    access_token = input("Inter access token:")
    url_to_fetch_from = "https://thanos-query-frontend-opf-observatorium.apps.smaug.na.operate-first.cloud"
    data_fetcher = DataFetcher(
        access_token=access_token,
        url_to_fetch_from=url_to_fetch_from
    )

    print(f"Connecting to url = {url_to_fetch_from}")
    data_fetcher.create_connection()
    print("Connection Successful!")

    number_of_retries = 10
    for attempt_number in range(1, number_of_retries + 1):
        try:
            data_fetcher.get_metric_data_for_the_past_number_of_hours()
            print("done fetching, nothing failed, exiting")
            break
        except Exception as e:
            print("Something went wrong in attempt number ", attempt_number, " retrying.\nThe error:")
            print(e)
            print("starting new attempt")
            print("+" * 200)


"""
***********************************************************************************************************************
    run main function
***********************************************************************************************************************
"""

if __name__ == "__main__":
    main()

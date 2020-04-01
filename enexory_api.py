'''
Enexory API examples

'''
import sys, os, logging
import datetime
import requests
import zlib
from io import StringIO
import pandas as pd
from datetime import timedelta

API_ENDPOINT = 'https://app.enexory.com/rest/'

def check_response_status(response):
    if response.status_code != 200:
        raise Exception("Response status code:{}\n\t{}".format(response.status_code, response.text))

def get_all_data_types(api_key):
    logging.info("Loading API data types with ids from %s", API_ENDPOINT)
    data = {"key": api_key, "method": "get_all_data_types"}
    response = requests.post(API_ENDPOINT,  json=data)
    check_response_status(response)
    content = response.content.decode("utf-8")
    lines = content.split("\n")
    data_name_by_id = {}
    header = True
    for line in lines:
        fields = line.split(';')
        if header:
            data_name_index = fields.index('data_name')
            api_id_index = fields.index('api_id')
        elif len(fields) > max([api_id_index, data_name_index]):
            api_id = fields[api_id_index]
            try:
                data_name_by_id[int(api_id)] = fields[data_name_index]
            except Exception as e:
                logging.warning("%s\n\t%s", str(e), fields)
        header = False
    return data_name_by_id

def get_data(api_key, data_ids, date_from, date_to):
    logging.info("Loading data ids %s from %s to %s from %s", str(data_ids), date_from, date_to, API_ENDPOINT)
    data = { "key": api_key, "method": "getdata", "data_ids": data_ids,
            "date_from": date_from if isinstance(date_from, str) else date_from.strftime('%Y-%m-%d'),
            "date_to": date_to if isinstance(date_to, str) else date_to.strftime('%Y-%m-%d')}
    response = requests.post(API_ENDPOINT,  json=data)
    check_response_status(response)
    csv_data = zlib.decompress(response.content, -zlib.MAX_WBITS)
    csv_data = csv_data.decode("utf-8")
    return csv_data

def main():
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    import argparse

    logging.info("python " + " ".join(sys.argv))

    parser = argparse.ArgumentParser()
    parser.add_argument("-ak", "--api_key", help="API key", required=True)
    parser.add_argument("-id", "--data_id", type = int, nargs='+', help="API data ids")

    def valid_date(s):
        try:
            return datetime.datetime.strptime(s, "%Y-%m-%d")
        except ValueError:
            raise argparse.ArgumentTypeError("Not a valid date: '{}'.".format(s))

    parser.add_argument("-from", "--date_from", help="Date interval start", type = valid_date,
                        default = (datetime.date.today()).strftime("%Y-%m-%d"))
    parser.add_argument("-to", "--date_to", help="Date interval end", type = valid_date,
                        default = (datetime.date.today() + datetime.timedelta(days = 10)).strftime("%Y-%m-%d"))

    args = parser.parse_args()

    # Example 1: load map of data ids to data names
    data_name_by_id = get_all_data_types(args.api_key)

    if args.data_id:
        # Example 2: print data in tabular separated format:
        data_names = [data_name_by_id[data_id] for data_id in args.data_id]
        logging.info("Querying data id(s):\n%s", str(zip(args.data_id, data_names)))
        csv_data = get_data(args.api_key, args.data_id, args.date_from, args.date_to)
        lines = csv_data.split("\n")
        for line in lines:
            print(line)

        # Example 3: convert data from original tabular form to a pandas dataframe:
        data = csv_data.replace(";\n", "\n")
        data = StringIO(data)
        index_col = 'date_time'
        df = pd.read_csv(data, sep=";", index_col=index_col, parse_dates=[index_col],
                         infer_datetime_format=True,
                         skiprows=1,
                         names=([index_col] + data_names))
        logging.info("Loaded dataframe of %s rows by %s columns ", df.shape[0], df.shape[1])
        print(df.describe())

        """
            Example 4: resample dataframe to desired timestep. 
            Various data series come from Enexory API with different time granularity (4 hours, 15 minutes, etc.)
            Resampling is a process of grouping original samples into a new time series with larger time step 
            using mean() or any other grouping function like min() or max().    
        """
        resampler = df.resample(timedelta(hours=4))
        df = resampler.mean()
        print(df.describe())

        """
            Example 5: Removing anomalies.
            Time series can contain values what abnormally differ from the most common data.
            Such anomalies can be caused by various stochastic reasons or just reflect a random deviation.
            In many cases it's considered as being useful to exclude anomalies from data series before using it
            for any data analysis.
            The code below excludes any samples what differ from series mean more than in 10 times of standard deviation.
        """
        for col in df:
            row_count = df.shape[0]
            describe = df[col].describe()
            mean = describe["mean"]
            max_dev = describe["std"] * 10
            df = df.loc[abs(df[col] - mean) < max_dev]
            new_row_count = df.shape[0]
            if new_row_count != row_count:
                print("Removed {} anomalies from column '{}'".format(row_count - new_row_count, col))
                print(df.describe())
            else:
                print("Could not find any anomalies on column '{}'".format(col))

        """
            Example 6: filling up gaps in values.
            For any certain date-time point, one series can contain some value whereas another series not.  
            So, when loading multiple series into a single dataframe, it can be useful to fill up the missing values.
            The code below fills empty cells by the last known preceding value.   
        """
        # fill all columns:
        df.fillna(method='ffill', inplace=True)
        # Try also 'bfill' to substitute empty cells with nearest known future value
        # Or fill only certain column: df['col_name'].fillna(method='ffill', inplace=True)
        print(df.describe())

    else:
        # Example: print out a full map of data ids and names
        import json
        print(json.dumps(data_name_by_id, indent=4, sort_keys=True, ensure_ascii=False))

if __name__ == '__main__':
    main()

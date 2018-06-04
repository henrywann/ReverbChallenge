import numpy as np
import json
from datetime import datetime as dt
import pandas as pd
import time


def populate_df(data='dataset.txt', labels=['url', 'timestamp', 'sessionId', 'experiments', 'userId'], datetime_format='%Y-%m-%dT%H:%M:%S.%f'):
    """Populates the data frame.

        Args:
            data (String): Path to the data set.
            labels (list): Names of the labels.
            datetime_format (String): Parsing datetime

        Returns:
            populated dataframe.

    """
    log = []
    with open(data, 'r') as data:
        line = data.readline()
        while line:
            try:
                current = json.loads(line)
            except ValueError:
                print('invalid json, continue to next line')
                line = data.readline()
                continue

            log.append((current[labels[0]], dt.strptime(current[labels[1]], datetime_format), current[labels[2]], current[labels[3]], current[labels[4]]))
            line = data.readline()

    df = pd.DataFrame.from_records(log, columns=labels)
    return df


def get_top_url(df, top=5):
    """Gets the top visited urls.

            Args:
                df (dataframe): input df.
                top (int): number of top urls.

            Returns:
                top visited urls.

    """

    url = df.loc[:, 'url'].as_matrix()
    url_counter = {}
    for element in url:
        if element in url_counter:
            url_counter[element] += 1
        else:
            url_counter[element] = 1
    most_frequent = sorted(url_counter, key=url_counter.get, reverse=True)[:top]

    return most_frequent


def get_stats(df):
    """Gets the statistics of the session duration times.

            Args:
                df (dataframe): input df.

            Returns:
                median, mean, min, max.

    """
    if df.empty:
        return 0, 0, 0, 0

    df = df.sort_values(by=['userId', 'timestamp'])
    duration = np.array([])
    ID = []
    st = df.iloc[0, 1]
    et = df.iloc[0, 1]
    currentUser = df.iloc[0, 4]
    previousUser = df.iloc[0, 4]
    init = True

    for index, row in df.iterrows():
        if row['sessionId'] in ID:
            continue
        else:
            if init:
                ID.append(row['sessionId'])
                init = False
                continue
            currentUser = row['userId']
            if currentUser == previousUser:  # ignores the last session of the user
                ID.append(row['sessionId'])
                et = row['timestamp']
                duration = np.append(duration, (et - st).days * 86400000 + (et - st).seconds * 1000 + (
                        et - st).microseconds / 100)
                st = row['timestamp']
            else:
                st = row['timestamp']
            previousUser = currentUser

    if len(duration) == 0:
        print('Not enough data point')
        return -1, -1, -1, -1

    return np.median(duration), np.mean(duration), np.min(duration), np.max(duration)


if __name__ == "__main__":

    '''
        Specify the path of the data set txt file and number of top visited url here
    '''
    data = 'dataset.txt'    # Path of the txt file
    datetime_format = '%Y-%m-%dT%H:%M:%S.%f'
    top = 5
    labels = ['url', 'timestamp', 'sessionId', 'experiments', 'userId']
    start_time = time.time()

    df = populate_df(data, labels, datetime_format)
    top_visited_url = get_top_url(df, top)
    median, mean, Min, Max = get_stats(df)

    elapsed_time = time.time() - start_time
    print("Finished in {} seconds".format(elapsed_time))

    results = {"most_viewed_urls": top_visited_url,
               "session_stats": {
                        "median": median,
                        "mean": mean,
                        "max": Max,
                        "min": Min
                    }
               }

    print(results)



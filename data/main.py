# import the required modules
import pandas as pd
import os
import glob
from datetime import datetime
import dask.dataframe as dd
from datetime import datetime, timedelta

# Use the pandas and dask libraries to read all the CSV files in the data directory.
# Concatenate them into a single DataFrame for data preprocessing.
data_path = "/Users/maejok/Downloads/2023-01-data-science-exercise-main/data/*"


def read_csv(data_path):
    ''''This function reads all the data in the folder data and its subfolders using Pandas and dask libraries and combines the individual CSV files into a single file.'''
    df_dict = []
    for path in sorted(glob.glob(data_path)):
        # read the data for each month using dask and merge it into a single dataframe
        df_dict.append(dd.read_csv(path + "/*.csv").compute())
    return pd.concat(df_dict)


df = read_csv(data_path)
print("DataFrame imported Succesfully. Shape of DataFrame is {}".format(df.shape))
# check for missing data
print(df.isna().sum() )


# Check the data types
print(df.info())
# Make a copy of the dataframe for preprocessing.
df1 = df.copy()


def preprocess(dataframe):
    '''This function (1) checks for any duplicate or irrelevant data and removes it.

    (2) checks for missing or null values and either removes the rows or replace them with appropriate values.

    (3) ensures that all the columns have consistent formatting, such as consistent date formats or naming conventions.

    After that, you can use a tool or programming language like Python's Pandas library to combine the individual CSV files into a single file.

    Finally, you can export the consolidated data to a new CSV file.'''
    # Reset the index
    dataframe.reset_index(inplace=True)
    dataframe = dataframe.drop_duplicates(keep='first')
    #Change the data type of the column 'observe_time' to string type.
    dataframe['observe_time'] = list(map(lambda x: str(x), dataframe['observe_time']))
    # make sure that each 'observe_time' has the format '2022-06-01 00:00:00+00:00'
    mask = dataframe['observe_time'].str.startswith("2022")

    df_filtered = dataframe[~mask]
    # If the dataframe.oberserve_time doesn't start with "2022" replace it the value with the correct format.
    if len(df_filtered) > 0:
        j = 0
        for i in df_filtered.index.tolist():
            dataframe.at[i, 'observe_time'] = str(df_filtered.iloc[j, 0][0])
            j += 1

    # generate all possible timedeltas with 10 minutes gap from start of June to end of September 2022.
    start_date = datetime.strptime('2022-06-01 00:00:00+00:00', '%Y-%m-%d %H:%M:%S%z')
    end_date = datetime.strptime('2022-09-30 23:50:00+00:00', '%Y-%m-%d %H:%M:%S%z')

    time_list = [str(datetime.strftime(start_date, '%Y-%m-%d %H:%M:%S%z'))[:-2] + ":00"]

    current_time = start_date

    while current_time <= end_date:
        time_list.append(str(datetime.strftime(current_time, '%Y-%m-%d %H:%M:%S%z'))[:-2] + ":00")
        current_time += timedelta(minutes=10)

    # Create a temporal dataframe of time_list
    temp = pd.DataFrame()
    temp["dates"] = time_list

    # Merge the first dataframe with the new dataframe of time list.
    merged_df = pd.merge(temp, dataframe.iloc[:, 1:], how='left', left_on=['dates'], right_on=['observe_time'])

    # Forward( and then backward fill) fill the missing values so that missing top of the hour value will be filled with 10minute prior(or after) to the hour.
    while merged_df.isna().sum()[-1] > 0:
        merged_df = merged_df.ffill(axis=0, limit=1).bfill(axis=0, limit=1)

    # Drop the column 'observe_time'  and rename "dates" to "observe_time"
    merged_df.drop('observe_time', axis=1, inplace=True)
    merged_df.rename(columns={'dates': 'observe_time'}, inplace=True)

    # Reduce the observe time to the format '%Y-%m-%d %H:%M'
    merged_df['observe_time'] = list(map(lambda x: str(x)[:16], merged_df['observe_time']))

    return merged_df[list(map(lambda x: str(x)[14:16] == "00", merged_df.observe_time))]


processed_df = preprocess(df1)
print("The new dataframe is processed. The shape is {}".format(processed_df.shape))

# define the new path to store the preprocessed files
data_path = "/Users/maejok/Downloads/2023-01-data-science-exercise-processed/data/"

df_2022_06 = processed_df[processed_df['observe_time'].str.startswith("2022-06")]
df_2022_06.to_csv(data_path + "df_2022_06.csv", index=False)
print("{} created. Shape is {}".format("df_2022_06", df_2022_06.shape))

df_2022_07 = processed_df[processed_df['observe_time'].str.startswith("2022-07")]
df_2022_07.to_csv(data_path + "df_2022_07.csv", index=False)
print("{} created. Shape is {}".format("df_2022_07", df_2022_07.shape))

df_2022_08 = processed_df[processed_df['observe_time'].str.startswith("2022-08")]
df_2022_08.to_csv(data_path + "df_2022_08.csv", index=False)
print("{} created. Shape is {}".format("df_2022_08", df_2022_08.shape))

df_2022_09 = processed_df[processed_df['observe_time'].str.startswith("2022-09")]
df_2022_09.to_csv(data_path + "df_2022_09.csv", index=False)
print("{} created. Shape is {}".format("df_2022_09", df_2022_09.shape))

print("All csv files saved to the data folder!")
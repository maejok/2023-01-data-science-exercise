# 2023-01 ICRAR Data Science Exercise


In this project, we  carried out exploratory data analysis on the given dataset. 

In the first part, we used PyCharm to produce "main.py" file.  We read the csv files in each folder using glob and dask and we merged all the csv files in a pandas datframe.  We  cleaned and consolidated CSV data files in a data directory and produced four output CSV files, one for each month, in the 'data'. We generated all time deltas with '10' minutes interval from '2022-06 00:00' to '2022-09 23:50'. We joinded the given data set to the time list and used forward and backward fill to replace missing data and retained the output files that has strictly hourly cadence, with 24 rows for every day in  each of the monthly file according to the given instruction.

In the second part, we used jupyter notebook and the appropriate visualisation tools. we first checked the overall distribution of the data and verify that there are no outliers or unexpected values. We also checked for any missing values and ensure that they were handled properly during the cleaning process.We created a histogram for the distribution of the averge of each name category.We also created a heatmap to check for any correlations between the columns. We then created time series sample plots and plots for the minimum, average and maximum values as well as moving avergaed and exponential smoothing to see how the values change over time. This allowed us to identify any patterns or trends in the data, as well as sudden changes or anomalies. This helped us to identify any relationships between the variables and see if there are dependencies or causalities.

Finally, we created a summary of the key findings and observations from the visualizations in a markdown cell to explain and describe the data.The specific python packages and tools we used include Matplotlib, seaborn and plotly.express which  always good choices for interactive data exploration and analysis. 

The folder is structured  as follows

```
2023-01-data-science-exercise/
├── README.md
├── data
│   ├── df_2022_06.csv
│   ├── df_2022_07.csv
│   ├── df_2022_08.csv
│   └── df_2022_09.csv
|   |---main.py
├── jupyter-notebook
│   └── jupyter-notebook.ipynb
├── requirements.txt
├── src
│   ├── __init__.py
│   └── code.py
└── tests
    ├── __init__.py
    └── test_code.py
```

To run the codes:
1. Clone the repository to your local machine using the command:
 git clone https://github.com/username/repositoryname.git

2. Navigate to the project directory by using the command:
cd repositoryname

3. Install the required dependencies by running:
pip install -r requirements.txt

4. Run the code by using the command:
python main.py --input_file input.txt --output_file output.txt

5. The output will be saved in the specified output file.


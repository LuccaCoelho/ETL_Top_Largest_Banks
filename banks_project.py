import sqlite3

import pandas as pd
import requests
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime


url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path = "./exchange_rate.csv"
table_attributes = ["Name", "MC_USD_Billion"]
db_name = "Banks.db"
table_name = "Largest_banks"
output_csv_path = "./Largest_banks_data.csv"

# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    """ This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing"""
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp_format = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as file:
        file.write(timestamp_format + ": " + message + "\n")

def extract(url, table_attribs):
    """ This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. """

    # get webpage
    web_page = requests.get(url).text

    # parse html
    data = BeautifulSoup(web_page, 'html.parser')

    # create an empty data frame
    df = pd.DataFrame(columns=table_attribs)

    # get corresponding table and rows
    table = data.find_all('tbody')
    rows = table[2].find_all('tr')

    # iterate over rows to get values
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            try:
                name = col[1].get_text(strip=True)
                mc_usd_billion = col[2].get_text(strip=True)

                # Handle cases where data might not be in expected format
                if name and mc_usd_billion:
                    data_dict = {"Name": name, "MC_USD_Billion": mc_usd_billion}
                    df = pd.concat([df, pd.DataFrame(data_dict, index=[0])], ignore_index=True)
            except Exception as e:
                print(f"Error processing row: {e}")

    return df

def transform(df, csv_path):
    """ This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies"""
    # read csv
    dataframe = pd.read_csv(csv_path)

    exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']

    # transform data into float
    usd_data = df["MC_USD_Billion"].tolist()
    usd_data = [float("".join(x.split(','))) for x in usd_data]

    # Create 3 rows with respective exchange rate
    df["MC_GBP_Billion"] = [np.round(x * exchange_rate["GBP"], 2) for x in usd_data]
    df["MC_EUR_Billion"] = [np.round(x * exchange_rate["EUR"], 2) for x in usd_data]
    df["MC_INR_Billion"] = [np.round(x * exchange_rate["INR"], 2) for x in usd_data]


    return df

def load_to_csv(df, output_path):
    """ This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing."""
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    """ This function saves the final data frame to a database
    table with the provided name. Function returns nothing."""
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)

def run_query(query_statement, sql_connection):
    """ This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. """
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


# Starting process
log_progress("Preliminaries complete. Initiating ETL process\n")
extracted_data = extract(url, table_attributes)
log_progress("Data extraction complete. Initiating Transformation process")

# Transforming data
dataframe = transform(extracted_data, csv_path)
log_progress("Data transformation complete. Initiating Loading process")

# Loading data to CSV
load_to_csv(dataframe, output_csv_path)
log_progress("Data saved to CSV file")

# Initialize connection to sql server
sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

# Load data to sql database
load_to_db(dataframe, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

# Initializing SQL Queries
select_all = "SELECT * FROM Largest_banks"
avg_mc_billion = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
top_5_banks = "SELECT Name from Largest_banks LIMIT 5"

# run queries
run_query(select_all, sql_connection)
run_query(avg_mc_billion, sql_connection)
run_query(top_5_banks, sql_connection)

log_progress("Process Complete\n")

sql_connection.close()
log_progress("Server Connection closed")



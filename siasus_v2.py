import logging
import pyodbc
import pandas as pd
import os 
import shutil
import dask.dataframe as dd
import glob

from tqdm import tqdm
from pysus.online_data.SIA import download, show_datatypes
from pysus.online_data import parquets_to_dataframe
from datetime import datetime, timedelta
from time import sleep
from pathlib import Path
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from constants import STATES, DIR_LIB, CLASS_DICT, STRING_CONNECTION, DIR_DOWNLOAD, QUERY_SQL
from dask.diagnostics import ProgressBar

def configure_logging() -> logging:
    """
    Configures the logging settings for the application.

    The function sets up a basic configuration for logging to a file named 'app.log'.
    It configures the logging level to DEBUG and uses a specific format for log messages.

    Returns:
        logging.Logger: The configured logger.

    Example:
        logger = configure_logging()    
        logger.debug('Debug message')
        logger.info('Information message')
        logger.warning('Warning message')
        logger.error('Error message')
        logger.critical('Critical message')
    """
    logging.basicConfig(filename='app.log', level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    #Obtém o logger padrão
    logger = logging.getLogger(__name__)
    return logger

def delete_files(
        download_dir: str = DIR_DOWNLOAD
) -> None:
    
    """
    Recursively removes all files and folders within the specified directory.

    Args:
        download_dir (str): The path of the directory to be cleaned. Default is DIR_DOWNLOAD.

    Retuns:
        None

    Example:
        delete_files('/path/to/directory')
        
    """
    for folder in os.listdir(download_dir): #11 40071790; 0800 646 8378; 7867751 protoloco xp
        folder_path = os.path.join(download_dir, folder)
        shutil.rmtree(folder_path)

    #print("Todas as pastas foram excluídas, incluindo arquivos e subdiretórios.")

def concat_and_filter(
        download_dir: str = DIR_DOWNLOAD,
        class_dict: str = CLASS_DICT
) -> pd.DataFrame:
    
    """
    Concatenates and filters data.
    
    This function reads parquet files from a specified directory, filters the data based on a provided class dictionary, and returns a pandas DataFrame.

    Args:
        download_dir (str): The directory from where the parquet files will be read. The default value is DIR_DOWNLOAD.
        class_dict (str): A dictionary that maps class names to procedure codes. The default value is CLASS_DICT.

    Returns:
        pd.DataFrame: A pandas DataFrame that contains the filtered data.

    Example:
        Here is an example of how to use this function::

            df = concat_and_filter(download_dir="my_directory", class_dict={"class1": ["code1", "code2"]})
    """

    all_paths = [f'{path}' for file_path in glob.glob(f"{download_dir}/*.parquet") for path in glob.glob(f'{file_path}/*.parquet')]

    # Criando um DataFrame vazio para armazenar os resultados
    final_filtered_df = pd.DataFrame()

    for file_path in tqdm(all_paths):
        #print(file_path)
        df = pd.read_parquet(file_path)

        filtered_values = df[df['PA_PROC_ID'].isin([item for sublist in class_dict.values() for item in sublist])].copy()
        
        filtered_values['CLASSE'] = filtered_values.apply(lambda row: next((class_name for class_name, class_codes in class_dict.items() if row['PA_PROC_ID'] in class_codes), None), axis=1)

        # Adding to the empty DataFrame
        final_filtered_df = pd.concat([final_filtered_df, filtered_values], ignore_index=True)

    # Creating columns: DATA_DOWNLOAD and DATA_PROCESSAMENTO.
    #final_filtered_df['DATA_DOWNLOAD'] = datetime.now().strftime("%Y-%m-%d")
    final_filtered_df['DATA_DOWNLOAD'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    final_filtered_df['DATA_PROCESSAMENTO'] = pd.to_datetime(final_filtered_df['PA_MVM'], format='%Y%m')
    final_filtered_df.to_csv(f'backup_datasus_{datetime.now().strftime("%d%m%y")}.csv', index=False)

    return final_filtered_df

def delete_table_sql(
        string_connection: tuple = STRING_CONNECTION,
        query: str = QUERY_SQL
    ) -> None:
    """
    Deletes a range from a table in the database.

    This function connects to a database using a provided connection string, executes an SQL query to delete a 5-month range from a table, and then closes the connection.

    Args:
        string_connection (tuple): A tuple containing the necessary information to connect to the database. The default value is STRING_CONNECTION.
        query (str): The SQL query that will be executed to delete the 5-month range from the table. The default value is QUERY_SQL.

    Returns:
        None

    Example:
        Here is an example of how to use this function::

            delete_table_sql(string_connection=("server", "database", "username", "password"), query="DELETE FROM table_name WHERE DATA_PROCESSAMENTO BETWEEN @DataMin AND @DataMax;
    """
    
    #Connect to the database
    connection_db = pyodbc.connect(string_connection)
    print("Connected to the database.")

    #Execute the query, commit and close the connection
    connection_db.execute(query)
    print('Database has been deleted')
    connection_db.commit()
    connection_db.close()

def update_data_sql(
        dataframe: pd.DataFrame,
        table_name: str = 'SUS_SIASUS',
        string_connection: tuple = STRING_CONNECTION
) -> None:
    
    """
    Updates a SQL table with data from a pandas DataFrame.

    This function connects to a database using a provided connection string, converts a pandas DataFrame to a SQL table, and then closes the connection.

    Args:
        dataframe (pd.DataFrame): The pandas DataFrame that contains the data to be added to the SQL table.
        table_name (str): The name of the SQL table that will be updated. The default value is 'SUS_SIASUS'.
        string_connection (tuple): A tuple containing the necessary information to connect to the database. The default value is STRING_CONNECTION.

    Returns:
        None

    Example:
        Here is an example of how to use this function::

            update_data_sql(dataframe=df, table_name="my_table", string_connection=("server", "database", "username", "password"))
    """
    
    conn = pyodbc.connect(string_connection)
    print('conexao ok')

    #Convert the dataframe to a sql table
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(string_connection))
    dataframe.to_sql(table_name, engine, if_exists='append', index=False)

    # Close the connection
    conn.close()

def download_files(
        #logger:logging.Logger,
        month_range: int = 6,
        num_months_ago: int = 3,
        id_acronym: str = 'PA',
        download_dir: str = DIR_DOWNLOAD,
        states: list = STATES
        ) -> None:
    
    """
    Downloads files for a range of months for specified states.

    This function calculates a range of months from the current date, and then downloads the corresponding files for each state in the provided list. The files are downloaded to a specified directory.

    Args:
        month_range (int): The number of months to include in the range. The default value is 6.
        num_months_ago (int): The number of months ago to start the range from. The default value is 3.
        id_acronym (str): The acronym for the group of files to download. The default value is 'PA'.
        download_dir (str): The directory where the downloaded files will be stored. The default value is DIR_DOWNLOAD.
        states (list): A list of states(UF) for which files will be downloaded. The default value is STATES.

    Returns:
        None

    Example:
        Here is an example of how to use this function::

            _download_files(month_range=6, num_months_ago=3, id_acronym='PA', download_dir='my_directory', states=['UF1', 'UF2'])
    """

    last_six_months = []
    current_date = datetime.now().replace(day=1) - relativedelta(months=num_months_ago)
    print('current_date2', current_date)
    for i in range(month_range):
        target_date = current_date - relativedelta(months=i)
        last_six_months.append(target_date)

    for uf in tqdm(states):

        #logger.info(f' --> Downloading UF: {uf} <-- ')
        print(uf)
        for month in last_six_months:
            year = month.year
            month_num = month.month
            #logger.info(f'Year-Month: {year}-{month_num}')
          
            download(states=uf, years=year, months=month_num, group=id_acronym, data_dir=download_dir)
    
        #logger.info('----------------------------------------/next-papge/-----------------------------------------------')
    

if __name__ == "__main__":
    #logger = configure_logging()
    download_files()
    delete_table_sql()
    dataframe = concat_and_filter()
    #dataframe = pd.read_csv('backup_datasus.csv', delimiter=',')
    update_data_sql(dataframe=dataframe)
    

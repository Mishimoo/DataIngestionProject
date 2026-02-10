import logging
import mysql.connector
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv("credentials.env")

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
#database configuration
DB_CONFIG = {
    'host': db_host,
    'user': db_user,
    'password': db_password
}
logging.basicConfig(filename='logs/dblog.log', filemode="a",\
    format="%(process)d - %(asctime)s - %(name)s - %(message)s", \
        datefmt= "%d-%b-%y %H:%M:%S",level = logging.DEBUG)
logger = logging.getLogger("DBlogger")
def start_DB():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            logger.info("Successfully connected to database")
            with connection.cursor() as cursor:
                cursor.execute("CREATE DATABASE IF NOT EXISTS mental_health_db")
                cursor.execute("USE mental_health_db")
                cursor.execute("""CREATE TABLE IF NOT EXISTS mental_health (indicator TEXT NOT NULL, category TEXT NOT NULL, state TEXT NOT NULL, \
                               subcategory TEXT NOT NULL, phase TEXT NOT NULL, time_period INT NOT NULL, time_period_label TEXT NOT NULL, time_period_start_date DATETIME NOT NULL,\
                                time_period_end_date DATETIME NOT NULL, value REAL, lowci REAL, highci REAL, confidence_interval TEXT, quartile_range TEXT)""")
                cursor.execute("""CREATE TABLE IF NOT EXISTS mental_health_rejected (indicator TEXT, category TEXT, \
                               state TEXT, subcategory TEXT, phase TEXT, time_period INT, time_period_label TEXT, \
                               time_period_start_date DATETIME, time_period_end_date DATETIME, value REAL, lowci REAL, highci REAL,\
                                confidence_interval TEXT, quartile_range TEXT, suppression_flag REAL, rejection_reason TEXT)""")
            return connection
        else:
            logger.warning("Failed to connect to DB. Check credentials and DB status and try again.")
    except mysql.connector.Error as err:
        logger.error(f"An error has occurred during connection: {err}")

def insert_valid_data(conn, df):
    data_to_insert = [tuple(row) for row in df.values]
    insert_query = f"""INSERT INTO mental_health (indicator, category, state , \
                               subcategory, phase, time_period, time_period_label, time_period_start_date,\
                                time_period_end_date, value, lowci, highci, confidence_interval, quartile_range) VALUES (%s, %s, %s, \
                               %s, %s, %s, %s, %s,\
                                %s, %s, %s, %s, %s, %s))"""
    try:
        conn.cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        logger.info(f"Succesfully inserted {len(data_to_insert)} rows into database")
    except mysql.connector.Error as err:
        logger.error(f"Error occurred during data insertion: {err}")

def insert_rejected_data(conn, df):
    data_to_insert = [tuple(row) for row in df.values]
    insert_query = f"""INSERT INTO mental_health_rejected (indicator, category, state , \
                               subcategory, phase, time_period, time_period_label, time_period_start_date,\
                                time_period_end_date, value, lowci, highci, confidence_interval, quartile_range,\
                                      suppression_flag, rejection_reason) VALUES (%s, %s, %s, \
                               %s, %s, %s, %s, %s,\
                                %s, %s, %s, %s, %s, %s, %s, %s))"""
    try:
        conn.cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        logger.info(f"Succesfully inserted {len(data_to_insert)} rows into rejected database")
    except mysql.connector.Error as err:
        logger.error(f"Error occurred during data insertion: {err}")

def close_connection(conn):
    try:
        conn.close()
        logger.info("Successfully closed DB connection")
    except mysql.connector.Error as err:
        logger.error(f"Error occurred when closing the connection: {err}")




#attempt to connect to the MySQL database


# conn = sqlite3.connect('testDB.db')
# c = conn.cursor()

# c.execute("""CREATE TABLE IF NOT EXISTS mental_health (indicator TEXT, category TEXT, state TEXT, subcategory TEXT, Phase TEXT, time_period REAL, time_period_label TEXT, time_period_start_date TEXT, time_period_end_date TEXT, value REAL, lowci REAL, highci REAL, confidence_interval TEXT, quartile_range TEXT)""")
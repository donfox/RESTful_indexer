########################################################################################################################
# db_utils.py
#
# Provides utility functions for interacting with a PostgreSQL database, including connecting, closing, 
# and executing queries.
#
# Developed by: Don Fox
# Date: 07/02/2024
#######################################################################################################################

import psycopg2
import logging
from config import DB_CONFIG


def connect_to_db():
    """
    Establish a connection to the PostgreSQL database using the configuration in DB_CONFIG.

    Returns:
        psycopg2.extensions.connection: A connection object to the PostgreSQL database if successful.
        None: If connection fails.
    """
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        print("Database connection successful.")
        return connection
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL database: {e}")
        return None


def close_db_connection(connection):
    """
    Close the provided database connection.

    If there are any uncommitted transactions, they are rolled back to avoid hanging.

    Args:
        connection (psycopg2.extensions.connection): The connection object to close.

    Returns:
        None
    """
    if connection:
        try:
            if connection.closed == 0:  # Connection is still open
                if connection.get_transaction_status() != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
                    connection.rollback() # Rollback any uncommitted transactions to prevent hanging

            connection.close()
            print("Database connection closed.")
        except Exception as e:
            logging.error(f"Error closing the database connection: {e}")


def perform_db_query(query, params=None):
    """
    Execute a SQL query on the database.

    Args:
        query (str): The SQL query to be executed.
        params (tuple, optional): Optional tuple of parameters to pass into a parameterized query.

    Returns:
        list or int or None:
            - If the query is a `SELECT`, returns a list of rows (list of tuples).
            - For `INSERT`, `UPDATE`, or `DELETE` queries, returns the number of affected rows (int).
            - None if the query fails.
    """
    connection = connect_to_db()
    if not connection:
        return None

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            if query.strip().lower().startswith("select"):
                result = cursor.fetchall()  # Fetch all rows for SELECT queries
            else:
                connection.commit()  # Commit the transaction for non-SELECT queries
                result = cursor.rowcount  # Return the number of rows affected
            # print(f"Query executed successfully: {query}")
            return result
    except psycopg2.Error as e:
        logging.error(f"Error executing query: {e}")
        return None
    finally:
        close_db_connection(connection)
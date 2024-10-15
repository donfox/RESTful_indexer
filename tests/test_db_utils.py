import pytest
from unittest.mock import patch, MagicMock
import psycopg2
from db_utils import connect_to_db, perform_db_query

# Mock for successful connection
@patch('psycopg2.connect')
def test_connect_to_db_success(mock_connect):
    mock_connection = MagicMock()  # Mock connection object
    mock_connect.return_value = mock_connection
    
    connection = connect_to_db()
    
    # Assert that psycopg2.connect() was called
    mock_connect.assert_called_once()
    
    # Assert that the returned connection is not None
    assert connection is not None


# Mock for failed connection
@patch('psycopg2.connect', side_effect=psycopg2.OperationalError)
def test_connect_to_db_failure(mock_connect):
    connection = connect_to_db()
    
    # Assert that psycopg2.connect() was called and returned None due to failure
    mock_connect.assert_called_once()
    assert connection is None


# Mock for successful query execution
@patch('db_utils.connect_to_db')
def test_perform_db_query_success(mock_connect_to_db):
    mock_connection = MagicMock()  # Mock connection
    mock_cursor = MagicMock()      # Mock cursor
    mock_connect_to_db.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    
    query = "SELECT * FROM blocks"
    
    # Call the function
    result = perform_db_query(query)
    
    # Assert that cursor.execute() was called with the correct query
    mock_cursor.execute.assert_called_once_with(query, None)
    
    # Assert that the connection and cursor were used
    mock_connection.cursor.assert_called_once()
    mock_cursor.fetchall.assert_called_once()


# Mock for query failure
@patch('db_utils.connect_to_db')
def test_perform_db_query_failure(mock_connect_to_db):
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    
    mock_connect_to_db.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    
    # Simulate an error during query execution
    mock_cursor.execute.side_effect = psycopg2.Error
    
    query = "SELECT * FROM blocks"
    
    result = perform_db_query(query)
    
    # Assert that the result is None due to query failure
    assert result is None
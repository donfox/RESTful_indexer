########################################################################################################################
#
# test_block_utils.py
#
#   functions: 
#       def test_fetch_block_success(mock_get)
#       def test_fetch_block_failure(mock_get)
#       def test_parse_and_store_block(mock_perform_db_query, mock_file_open)
#                                                                                                                     
#######################################################################################################################
import pytest
import sys
import os

# Add the extraction_transformation directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../extraction_transformation')))
from block_utils import fetch_block, parse_and_store_block
from unittest.mock import patch, mock_open


# Test for fetch_block function
@patch('requests.get')
def test_fetch_block_success(mock_get):
    # Simulate a successful response with mock JSON data
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"block": {"header": {"height": 123}, "data": {"txs": []}}, "block_id": {"hash": "abc123"}}
    
    url = "https://example.com/block"
    result = fetch_block(url)
    
    assert result is not None
    assert result["block"]["header"]["height"] == 123
    assert result["block_id"]["hash"] == "abc123"


@patch('requests.get')
def test_fetch_block_failure(mock_get):
    # Simulate a failed response
    mock_get.return_value.status_code = 404
    mock_get.return_value.json.return_value = None
    
    url = "https://example.com/block"
    result = fetch_block(url)
    
    assert result is None


# Test for parse_and_store_block function
@patch('builtins.open', new_callable=mock_open)
@patch('block_utils.perform_db_query')
def test_parse_and_store_block(mock_perform_db_query, mock_file_open):
    block_data = {
        "block": {
            "header": {
                "height": 123,
                "time": "2023-09-18T20:11:58.302715257Z"
            },
            "data": {
                "txs": ["tx1", "tx2"]
            }
        },
        "block_id": {
            "hash": "abc123"
        }
    }
    
    local_block_repository = './blocks_store'
    
    # Call the function
    parse_and_store_block(block_data, 123, "abc123", "2023-09-18T20:11:58.302715257Z", local_block_repository)
    
    # Check that the perform_db_query was called with the correct SQL query
    assert mock_perform_db_query.call_count == 3  # Once for block, twice for transactions
    
    # Verify the first call was to insert the block
    block_insert_call = mock_perform_db_query.call_args_list[0]

    # Strip the query to remove any leading/trailing whitespace and newlines
    assert block_insert_call[0][0].strip().startswith("INSERT INTO blocks")

    # Verify the next two calls were to insert transactions
    tx_insert_call_1 = mock_perform_db_query.call_args_list[1]
    tx_insert_call_2 = mock_perform_db_query.call_args_list[2]

    # Strip the query to remove any leading/trailing whitespace and newlines
    assert tx_insert_call_1[0][0].strip().startswith("INSERT INTO transactions")
    assert tx_insert_call_2[0][0].strip().startswith("INSERT INTO transactions")


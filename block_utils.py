#######################################################################################################################
# block_utils.py
#
# This module provides utilities to interact with the blockchain API, process block data, and handle missing blocks.
# It fetches, parses, and stores blocks, detects missing blocks, and interacts with Redis for storing and retrieving
# block-related information. Additionally, the module provides functions for handling and filling in gaps in the 
# block sequence.
#
# Features:
#     - Fetch block data from the blockchain API.
#     - Parse and store block data in the database.
#     - Detect and handle missing blocks.
#     - Use Redis to track processed and missing blocks.
#
# Developed by: Don Fox
# Date: 07/02/2024
#######################################################################################################################

import requests
import logging
import time
from db_utils import perform_db_query
from redis_utils import clear_missing_blocks, store_missing_blocks, get_redis_connection, get_missing_blocks
from config import DB_CONFIG, LATEST_BLOCK_URL, BLOCK_CHAIN_URL_TEMPLATE, NUM_BLOCKS_TO_FETCH

def fetch_block(url):
    """
    Fetch block data from the specified URL.

    Args:
        url (str): The URL from which to fetch the block data.

    Returns:
        dict: The JSON response containing block data, or None in case of an error.
    """
    try:
        response = requests.get(url, timeout=12)
        response.raise_for_status()  # Raise an exception for 4xx/5xx responses
        return response.json()
        logging.info(f"Fetched block {block_height} from API.")
        return block_data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching block: {e}")
        return None  


def parse_and_store_block(block_data, block_height, block_hash, timestamp):
    """
    Parse block data and store relevant information in the database.

    Args:
        block_data (dict): The block data dictionary fetched from the blockchain.
        block_height (int): The block height that has already been parsed.
        block_hash (str): The block hash that has already been parsed.
        timestamp (str): The timestamp of the block.

    Returns:
        None
    """
    if not block_data:
        logging.error("No block data to parse.")
        return

    # Store the block and transaction details in the database
    query_block = """
        INSERT INTO blocks (block_height, block_hash, timestamp)
        VALUES (%s, %s, %s)
        ON CONFLICT (block_height) DO NOTHING
    """
    perform_db_query(query_block, (block_height, block_hash, timestamp))

    # Store each transaction associated with the block
    transactions = block_data.get("block", {}).get("data", {}).get("txs", [])
    for tx_hash in transactions:
        query_tx = """
            INSERT INTO transactions (tx_hash, block_id)
            VALUES (%s, (SELECT id FROM blocks WHERE block_height = %s))
            ON CONFLICT (tx_hash) DO NOTHING
        """
        perform_db_query(query_tx, (tx_hash, block_height))

    logging.info(f"Block {block_height} and its transactions have been processed.")


def detect_missing_blocks(redis_conn):
    """
    Detect gaps in the sequence of processed blocks stored in Redis.

    Args:
        redis_conn (redis.Redis): The Redis connection object used to retrieve processed blocks.

    Returns:
        list[int]: A list of missing block heights.
    """
    processed_blocks = redis_conn.smembers('processed_blocks')  # This set should not be cleared

    if not processed_blocks:
        logging.info("No blocks found in Redis to check for gaps.")
        return []

    processed_blocks = sorted(list(map(int, processed_blocks)))   # Variable name change?

    missing_blocks = []
    for i in range(1, len(processed_blocks)):
        current_block = processed_blocks[i]
        previous_block = processed_blocks[i - 1]
        if current_block - previous_block > 1:
            missing_blocks.extend(range(previous_block + 1, current_block))

        if missing_blocks:
            logging.info(f"Detected missing blocks: {missing_blocks}")
            store_missing_blocks(redis_conn, missing_blocks)  # Store detected missing blocks in Redis
        
        return missing_blocks


def request_missing_blocks(missing_blocks, redis_conn) -> None:
    """
    Request and process missing blocks from the blockchain API.

    Args:
        missing_blocks (list[int]): List of missing block heights to be fetched and processed.
        redis_conn (redis.Redis): Redis connection object used to track block statuses.

    Returns:
        None
    """
    if not missing_blocks:
        logging.info("No missing blocks detected.")
        return 

    store_missing_blocks(redis_conn, missing_blocks)
    stored_missing_blocks = list(map(int, get_missing_blocks(redis_conn)))  # Ensure blocks are handled as integers

    for block in stored_missing_blocks:
        block_url = BLOCK_CHAIN_URL_TEMPLATE.format(block)
        block_data = fetch_block(block_url)

        if block_data:
            process_block(block_data, redis_conn)
        else:
            logging.error(f"Error fetching block from URL: {block_url}")


def process_block(block_data, redis_conn) -> bool:
    """
    Process and store block data, marking it as processed in Redis.

    Args:
        block_data (dict): The block data dictionary fetched from the blockchain.
        redis_conn (redis.Redis): Redis connection object used to track processed blocks.

    Returns:
        bool: True if the block was successfully processed, False otherwise.
    """
    block_height = block_data.get("block", {}).get("header", {}).get("height")

    # Check whether block has already been processed.
    if redis_conn.sismember('processed_blocks', block_height):
        logging.info(f"Block {block_height} has already been processed, skipping...")
        return False

    block_hash = block_data.get("block_id", {}).get("hash")
    timestamp = block_data.get("block", {}).get("header", {}).get("time")

    # Store the block and its transactions in the DB
    parse_and_store_block(block_data, block_height, block_hash, timestamp)

    # Mark the block as processed
    result = redis_conn.sadd('processed_blocks', block_height)
    if result:
        logging.info(f"Marked block {block_height} as processed and added to Redis")
    else:
        logging.error(f"Failed to add block {block_height} to Redis processed_blocks set.")

    # Check if the block is in missing_blocks before removing it
    if redis_conn.sismember('missing_blocks', block_height):
        removed = redis_conn.srem('missing_blocks', block_height)
        if removed:
            logging.info(f"Block {block_height} successfully removed from missing blocks.")
        else:
            logging.error(f"Failed to remove block {block_height} from missing blocks.")
    else:
        logging.info(f"Block {block_height} was not found in missing blocks, skipping removal.")

    return True


def extract_current_blocks(redis_conn) -> bool:
    """
    Extract and process the latest blocks from the blockchain API.

    Fetches and processes up to a defined number of blocks (NUM_BLOCKS_TO_FETCH) from the blockchain,
    ensuring blocks are processed in sequence and avoiding duplication.

    Args:
        redis_conn (redis.Redis): Redis connection object used to track processed blocks.

    Returns:
        None
    """ 
    blocks_cntr = 0
    logging.info("Starting to extract blocks...")

    while blocks_cntr < NUM_BLOCKS_TO_FETCH:

        if blocks_cntr >= NUM_BLOCKS_TO_FETCH:
            logging.info(f"Completed fetching {NUM_BLOCKS_TO_FETCH} blocks. Exiting extraction.")
            break

        block_data = fetch_block(LATEST_BLOCK_URL)  # Fetch the latest block

        if block_data:
            if process_block(block_data, redis_conn):
                blocks_cntr += 1
                logging.info(f"Processed block {blocks_cntr}/{NUM_BLOCKS_TO_FETCH}")
            else:
                logging.error("Block was not processed, skipping counter increment")
            time.sleep(3)
        else:
            logging.error("Error: Failed to fetch the latest block.")

        logging.info(f"Completed fetching {NUM_BLOCKS_TO_FETCH} blocks. Exiting extraction.")

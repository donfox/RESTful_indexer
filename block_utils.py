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
from config import BLOCK_FETCH_DELAY
from db_utils import perform_db_query
from redis_utils import redis, \
                        clear_missing_blocks, \
                        store_missing_blocks, \
                        get_redis_connection, \
                        get_missing_blocks

from config import DB_CONFIG, \
                   LATEST_BLOCK_URL, \
                   BLOCK_CHAIN_URL_TEMPLATE, \
                   NUM_BLOCKS_TO_FETCH

def fetch_block(url: str) -> dict:
    """Fetch block data from a URL as JSON."""
    try:
        response = requests.get(url, timeout=12)
        response.raise_for_status()  # Raise an exception for 4xx/5xx responses
        
        block_data = response.json()
        logging.info(f"Successfully fetched block data from {url}.")
        return block_data

    except requests.exceptions.Timeout:
        logging.error(f"Request timed out while fetching block data from {url}.")
    except requests.exceptions.ConnectionError:
        logging.error(f"Connection error occurred while accessing {url}.")
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while fetching block data from {url}: {e}")
    
    return None

def parse_and_store_block(block_data: dict, block_height: int, block_hash: str, timestamp: str) -> None:
    """Parse block data and store relevant information in the database."""
    if not block_data:
        logging.error(f"No block data provided for block height {block_height}.")
        return

    try:
        # Insert block metadata
        query_block = """
            INSERT INTO blocks (block_height, block_hash, timestamp)
            VALUES (%s, %s, %s)
            ON CONFLICT (block_height) DO NOTHING
        """
        perform_db_query(query_block, (block_height, block_hash, timestamp))
        logging.info(f"Block {block_height} metadata stored in the database.")
    except Exception as e:
        logging.error(f"Failed to insert block {block_height} into the database: {e}")
        return

    # Process transactions
    transactions = block_data.get("block", {}).get("data", {}).get("txs", [])
    if not transactions:
        logging.info(f"No transactions found for block {block_height}.")
        return

    try:
        # Batch insert transactions
        query_tx = """
            INSERT INTO transactions (tx_hash, block_id)
            VALUES %s
            ON CONFLICT (tx_hash) DO NOTHING
        """
        transaction_values = [(tx_hash, block_height) for tx_hash in transactions]
        perform_db_query(query_tx, transaction_values, batch=True)
        logging.info(f"{len(transactions)} transactions for block {block_height} stored in the database.")
    except Exception as e:
        logging.error(f"Failed to insert transactions for block {block_height}: {e}")


# def parse_and_store_block(block_data, block_height, block_hash, timestamp):
#     """
#     Parse block data and store relevant information in the database.

#     Args:
#         block_data (dict): The block data dictionary fetched from the blockchain.
#         block_height (int): The block height that has already been parsed.
#         block_hash (str): The block hash that has already been parsed.
#         timestamp (str): The timestamp of the block.

#     Returns:
#         None
#     """
#     if not block_data:
#         logging.error("No block data to parse.")
#         return

#     # Store the block and transaction details in the database
#     query_block = """
#         INSERT INTO blocks (block_height, block_hash, timestamp)
#         VALUES (%s, %s, %s)
#         ON CONFLICT (block_height) DO NOTHING
#     """
#     perform_db_query(query_block, (block_height, block_hash, timestamp))

#     # Store each transaction associated with the block
#     transactions = block_data.get("block", {}).get("data", {}).get("txs", [])
#     for tx_hash in transactions:
#         query_tx = """
#             INSERT INTO transactions (tx_hash, block_id)
#             VALUES (%s, (SELECT id FROM blocks WHERE block_height = %s))
#             ON CONFLICT (tx_hash) DO NOTHING
#         """
#         perform_db_query(query_tx, (tx_hash, block_height))

#     logging.info(f"Block {block_height} and its transactions have been processed.")


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
    print(f"In detect_missing_blocks: processed_blocks:{processed_blocks}")

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


def request_missing_blocks(missing_blocks: list[int], redis_conn: redis.Redis) -> None:
    """Request and process/store missing blocks from the blockchain API."""

    if not missing_blocks:
        logging.info("No missing blocks to process.")
        return

    logging.info(f"Processing missing blocks: {missing_blocks}")

    # Store missing blocks in Redis
    store_missing_blocks(redis_conn, missing_blocks)
    stored_missing_blocks = list(map(int, get_missing_blocks(redis_conn)))  # Ensure blocks are integers

    for block in stored_missing_blocks:
        try:
            block_url = BLOCK_CHAIN_URL_TEMPLATE.format(block)
            block_data = fetch_block(block_url)

            if block_data:
                process_block(block_data, redis_conn)
                logging.info(f"Successfully processed block: {block}")
            else:
                logging.error(f"Failed to fetch block from URL: {block_url}")
        except Exception as e:
            logging.error(f"Error processing block {block}: {e}")


def process_block(block_data: dict, redis_conn: redis.Redis) -> bool:
    """Process and store block data, marking it as processed in Redis."""

    block_height = block_data.get("block", {}).get("header", {}).get("height")

    if block_height is None:
        logging.error("Block height is missing or invalid in block data. Skipping processing.")
        return False

    # Check if the block has already been processed
    if redis_conn.sismember('processed_blocks', block_height):
        logging.info(f"Block {block_height} has already been processed, skipping...")
        return False

    block_hash = block_data.get("block_id", {}).get("hash")
    timestamp = block_data.get("block", {}).get("header", {}).get("time")

    if not block_hash or not timestamp:
        logging.error(f"Block {block_height} is missing critical data (hash or timestamp). Skipping.")
        return False

    # Process the block: parse and store in the database
    try:
        parse_and_store_block(block_data, block_height, block_hash, timestamp)
    except Exception as e:
        logging.error(f"Failed to process and store block {block_height}: {e}")
        return False

    # Mark the block as processed in Redis
    if redis_conn.sadd('processed_blocks', block_height):
        logging.info(f"Marked block {block_height} as processed in Redis.")
    else:
        logging.error(f"Failed to add block {block_height} to Redis processed_blocks set.")
        return False

    # Remove the block from missing_blocks if it exists
    if redis_conn.sismember('missing_blocks', block_height):
        if redis_conn.srem('missing_blocks', block_height):
            logging.info(f"Block {block_height} removed from missing_blocks.")
        else:
            logging.error(f"Failed to remove block {block_height} from missing_blocks.")
    else:
        logging.info(f"Block {block_height} was not found in missing_blocks, skipping removal.")

    return True


def extract_current_blocks(redis_conn: redis.Redis) -> None:
    """Extract and process the latest blocks from the blockchain API."""
    blocks_cntr = 0
    logging.info("Starting to extract blocks...")

    while blocks_cntr < NUM_BLOCKS_TO_FETCH:
        try:
            block_data = fetch_block(LATEST_BLOCK_URL)  # Fetch the latest block

            if not block_data:
                logging.error("Error: Failed to fetch the latest block. Retrying...")
                continue

            if process_block(block_data, redis_conn):
                blocks_cntr += 1
                logging.info(f"Processed block {blocks_cntr}/{NUM_BLOCKS_TO_FETCH}")
            else:
                logging.error("Block was not processed, skipping counter increment")

            time.sleep(BLOCK_FETCH_DELAY)  # Use configurable delay
        except Exception as e:
            logging.error(f"Unexpected error during block extraction: {e}")
            break

    logging.info(f"Completed fetching {blocks_cntr}/{NUM_BLOCKS_TO_FETCH} blocks. Exiting extraction.")

"""
***********************************************************************
main.py -- RESTful_BLOCK_EXTRACTOR

This script extracts blockchain blocks from an online API and stores 
them in a database. It performs the following functionalities:
    - Initially performs a comprehensive gap check, filling in missing blocks.
    - Handles error cases and retry mechanisms during the process.

Configuration:
    - local_block_repository: Path to the local directory for storing blocks (if enabled).
    - latest_block_uri: URL for fetching the latest block.

Developed by: Don Fox
Date: 07/02/2024
***********************************************************************
"""

import block_utils
import redis_utils
import config
import logging

def detect_and_fetch_missing_blocks(redis_conn) -> None:
    """
    Detects and processes missing blockchain blocks.

    This function detects missing blocks from the `processed_blocks` set in Redis
    and fetches them using utility functions.
    Args:
        redis_conn (redis.Redis): Redis connection object to interact with the Redis database.

    Returns:
        None
    """
    try:
        missing_blocks = block_utils.detect_missing_blocks(redis_conn)
        if missing_blocks:
            block_utils.request_missing_blocks(missing_blocks, redis_conn)
            logging.info(f"MISSING BLOCKS: {missing_blocks}")
        else:
            logging.info("No missing blocks detected.")
    except Exception as e:
        logging.error(f"Error detecting or fetching missing blocks: {e}")
        
def main():
    """
    Iinitialize Redis connection and process blockchain blocks.

    Establishes a connection to Redis, processes missing blocks, and extracts
    the latest blockchain blocks.

    Returns:
        None
    """
    try:
        redis_conn = redis_utils.get_redis_connection()
        detect_and_fetch_missing_blocks(redis_conn)
        block_utils.extract_current_blocks(redis_conn)
    except Exception as e:
        logging.error(f"Error in main processing: {e}")


if __name__ == '__main__':
    main()




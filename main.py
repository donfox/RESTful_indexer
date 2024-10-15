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

def detect_and_fetch_missing_blocks(redis_conn):
    """
    Detects and processes missing blockchain blocks.

    This function first detects missing blocks from the `processed_blocks` set in Redis
    by calling the `block_utils.detect_missing_blocks()` function. Then, it fetches and 
    processes those missing blocks by calling `block_utils.request_missing_blocks()`.

    Args:
        redis_conn (redis.Redis): Redis connection object to interact with the Redis database.

    Returns:
        None
    """
    missing_blocks = block_utils.detect_missing_blocks(redis_conn)

    if missing_blocks:
        block_utils.request_missing_blocks(missing_blocks, redis_conn)
    
    logging.info(f"MISSING BLOCKS: {missing_blocks}")


def main():
    """
    Main function to initialize Redis connection and process blockchain blocks.

    This function establishes a connection to Redis, detects and processes any 
    missing blockchain blocks, then extracts the latest blockchain blocks by 
    calling the appropriate utility functions from `block_utils` and `redis_utils`.

    Returns:
        None
    """
    redis_conn = redis_utils.get_redis_connection()

    detect_and_fetch_missing_blocks(redis_conn)
    block_utils.extract_current_blocks(redis_conn)
        

if __name__ == '__main__':
    main()




"""
#######################################################################################################################
# redis_utils.py
#
# Provides functions to interact with Redis for saving and retrieving information about missing blocks 
# or gaps in the block sequence. Includes utility functions to store, retrieve, and clear missing blocks.
#
# Developed by: Don Fox
# Date: 07/02/2024
#######################################################################################################################
"""

import redis
import logging
from redis import Redis
from redis.exceptions import RedisError
from config import REDIS_CONFIG

def get_redis_connection() -> Redis:
    """
    Establishes a connection to Redis using the configuration provided in REDIS_CONFIG.

    Returns:e
        Redis: The Redis connection object.
    
    Raises:
        RedisError: If unable to connect to Redis.
    """
    try:
        conn = redis.Redis(**REDIS_CONFIG)
        conn.ping()  # Test the connection
        logging.info(f"Connected to Redis at {REDIS_CONFIG['host']}:{REDIS_CONFIG['port']}, DB: {REDIS_CONFIG['db']}")
        return conn
    except redis.ConnectionError as e:
        logging.error(f"Failed to connect to Redis: {e}")
        raise


def store_missing_blocks(redis_conn: Redis, missing_blocks: list[int]) -> None:
    """
    Stores a list of missing block numbers in Redis.

    Args:
        redis_conn (Redis): Redis connection object.
        missing_blocks (list[int]): List of missing block numbers.

    Returns:
        None
    """
    if missing_blocks:
        try:
            with redis_conn.pipeline() as pipe:  # Batch Redis operations using pipeline
                for block in missing_blocks:
                    pipe.sadd('missing_blocks', block)
                pipe.execute()
            logging.info(f"Stored {len(missing_blocks)} missing blocks in Redis.")
        except RedisError as e:
            logging.error(f"Failed to store missing blocks in Redis: {e}")
    else:
        logging.info("No missing blocks to store.")


def get_missing_blocks(redis_conn: Redis) -> list[int]:
    """
    Retrieves the list of missing blocks from Redis.

    Args:
        redis_conn (Redis): Redis connection object.

    Returns:
        list[int]: List of missing block numbers (decoded from bytes to integers).
    """
    try:
        missing_blocks = redis_conn.smembers('missing_blocks')
        return [int(block.decode('utf-8')) for block in missing_blocks]
    except RedisError as e:
        logging.error(f"Failed to retrieve missing blocks from Redis: {e}")
        return []


def clear_missing_blocks(redis_conn: Redis) -> None:
    """
    Clears the set of missing blocks from Redis.

    Args:
        redis_conn (Redis): Redis connection object.

    Returns:
        None
    """
    try:
        redis_conn.delete('missing_blocks')
        logging.info("Successfully cleared missing blocks from Redis.")
    except RedisError as e:
        logging.error(f"Failed to clear missing blocks from Redis: {e}")





        

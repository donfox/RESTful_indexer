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
from redis.exceptions import RedisError, ConnectionError, TimeoutError, AuthenticationError
from config import REDIS_CONFIG
import time
from tenacity import retry, stop_after_attempt, wait_exponential


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
def get_redis_connection() -> Redis:
    """Connect to Redis using REDIS_CONFIG."""
    try:
        conn = redis.Redis(**REDIS_CONFIG)   # Create Redis connection with provided config
        conn.ping()  # Test the connection
        logging.info(f"Connected to Redis at {REDIS_CONFIG['host']}:{REDIS_CONFIG['port']}, DB: {REDIS_CONFIG['db']}")
        return conn

    except AuthenticationError as auth_err:
        logging.error("Redis authentication failed. Check your credentials in REDIS_CONFIG.")
        raise auth_err

    except TimeoutError as timeout_err:
        logging.error("Redis connection timed out. Ensure Redis is reachable or adjust timeout settings.")
        raise timeout_err

    except ConnectionError as conn_err:
        logging.error("Failed to connect to Redis. Verify that the Redis server is running and accessible.")
        raise conn_err

    except RedisError as redis_err:
        logging.error(f"An unexpected Redis error occurred: {redis_err}")
        raise redis_err


def store_missing_blocks(redis_conn: Redis, missing_blocks: list[int]) -> None:
    """Store a list of missing block numbers in Redis."""
    if not missing_blocks:
        logging.info("No missing blocks to store.")
        return
    try:
        with redis_conn.pipeline() as pipe:  # Batch Redis operations
            for block in missing_blocks:
                pipe.sadd('missing_blocks', block)
            pipe.execute()
        logging.info(f"Stored {len(missing_blocks)} missing blocks in Redis: {missing_blocks}")
    except RedisError as e:
        logging.error(f"Failed to store missing blocks in Redis: {missing_blocks}. Error: {e}")


def get_missing_blocks(redis_conn: Redis) -> list[int]:
    """Retrieves list of missing blocks (decoded from bytes to integers) from Redis ."""
    try:
        missing_blocks = redis_conn.smembers('missing_blocks')
        return [int(block.decode('utf-8')) for block in missing_blocks]
    except RedisError as e:
        logging.error(f"Failed to retrieve missing blocks from Redis: {e}")
        return []


def clear_missing_blocks(redis_conn: Redis) -> None:
    """Clears the set of missing blocks from Redis."""
    try:
        redis_conn.delete('missing_blocks')
        logging.info("Successfully cleared missing blocks from Redis.")
    except RedisError as e:
        logging.error(f"Failed to clear missing blocks from Redis: {e}")





        

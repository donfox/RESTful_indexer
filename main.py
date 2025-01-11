"""
********************************************************************************
main.py -- RESTful_BLOCK_EXTRACTOR

This script extracts blockchain blocks from an online API and stores 
them in a database. It performs the following functionalities:
    - Initially performs a comprehensive gap check, filling in missing blocks.
    - Handles error cases and retry mechanisms during the process.

Configuration:
    - local_block_repository: Path to the local directory for storing blocks 
        (if enabled).
    - latest_block_uri: URL for fetching the latest block.

Developed by: Don Fox
Date: 07/02/2024
********************************************************************************
"""
from pycallgraph2 import PyCallGraph
from pycallgraph2.output import GraphvizOutput

import sys
sys.stdout.reconfigure(line_buffering=True)
import logging
import config

from utils import detect_missing_blocks, request_missing_blocks,\
                  get_redis_connection, extract_current_blocks
from redis import Redis

def detect_and_fetch_missing_blocks(redis_conn: Redis) -> None:
    """Fix gaps detected in collected blockchain blocks."""
    try:
        missing_blocks = detect_missing_blocks(redis_conn)
        if not missing_blocks:
            logging.info("No missing blocks detected.")
            return
        logging.info(f"Missing Blocks: {missing_blocks}")
        request_missing_blocks(missing_blocks, redis_conn)
        logging.info(f"Fetched missing blocks: {missing_blocks}")

    except Redis.ConnectionError as conn_err:
        logging.error(f"Redis connection error: {conn_err}")
    except KeyError as key_err:
        logging.error(f"Key error while processing missing blocks: {key_err}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")


def main():
    """
    Initialize Redis connection fixes gaps in collected blocks, then extracts 
    the latest blockchain blocks.
    """
    try:
        redis_conn = get_redis_connection()
        detect_and_fetch_missing_blocks(redis_conn)
        extract_current_blocks(redis_conn)
    except Exception as e:
        logging.error(f"Error in main processing: {e}")


if __name__ == '__main__':
   
    main()

    # Generates graph of applications function calls.
    # graphviz = GraphvizOutput()
    # graphviz.output_file = 'call_graph.png'
    #
    # with PyCallGraph(output=graphviz):
    #     main()




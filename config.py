"""
***********************************************************************
config.py -- Application Configuration

This script defines global settings for logging, database connection, 
Redis configuration, and API endpoints used by the blockchain extractor.

Features:
    - Configures logging for the entire application.
    - Sets database and Redis connection details using environment variables or defaults.
    - Defines URLs for fetching blockchain data from an API.

Developed by: Don Fox
Date: 07/02/2024
***********************************************************************
"""

import os
import redis
import logging

# Configure logging globally for the application
logging.basicConfig(
    filename='../logs/block_requests.log', 
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)

# Database Configuration
DB_CONFIG = {
    "database": os.getenv("DB_NAME", "blockchain"),
    "user": os.getenv("DB_USER", "donfox1"),
    "password": os.getenv("DB_PASSWORD", "xofnod"),  # Preferably use env vars for sensitive info
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
    
}

# Redis Configuration
REDIS_CONFIG = {
    "host": os.getenv("REDIS_HOST", 'localhost'),
    "port": int(os.getenv("REDIS_PORT", 6379)),
    "db": int(os.getenv("REDIS_DB", 0)),
}

# Blockchain API URLs
LATEST_BLOCK_URL = "https://migaloo-api.polkachu.com/cosmos/base/tendermint/v1beta1/blocks/latest"
BLOCK_CHAIN_URL_TEMPLATE = "http://116.202.143.93:1317/cosmos/base/tendermint/v1beta1/blocks/{}"

# Application Settings
NUM_BLOCKS_TO_FETCH = int(os.getenv("NUM_BLOCKS_TO_FETCH", 6))
# Blockchain Indexer Project

This project is a blockchain indexer that extracts block files from an online API, transforms them 
into JSON format, and stores the data in a local PostgreSQL database. The indexer also ensures data 
integrity by detecting and filling in missing blocks, providing robust error handling, and logging 
the entire process.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Design Steps](#design-steps)
   - [1. Extraction](#1-extraction)
   - [2. Transformation](#2-transformation)
   - [3. Loading](#3-loading)
   - [4. Database Design](#4-database-design)
   - [5. Error Handling and Logging](#5-error-handling-and-logging)
   - [6. Testing](#6-testing)
   - [7. Documentation](#7-documentation)
3. [How to Run the Project](#how-to-run-the-project)
4. [Future Improvements](#future-improvements)
5. [Conclusion](#conclusion)

---

## Project Overview

This project demonstrates the creation of a blockchain indexer in Python. It interacts with a 
blockchain API to fetch, process, and store blocks in a PostgreSQL database. The main steps include:

- **Fetching** data from a remote API.
- **Transforming** the raw data into a structured JSON format.
- **Storing** the transformed data in a PostgreSQL database.

The project is designed with modularity, error handling, and logging in mind, making it easy to 
maintain and extend.

---

## Design Steps

### 1. Extraction
- **Description**: In this step, we extract block data from a blockchain API. The indexer sends HTTP
    requests to the API, manages responses, and handles issues like rate limits or timeouts.
- **Implementation Considerations**:
  - Use Python's `requests` library to fetch data from the blockchain API.
  - Implement retries and backoff strategies for handling network issues.
  - Ensure the application can fetch data in batches to improve efficiency.

### 2. Transformation
- **Description**: The extracted raw data is transformed into a structured format (JSON), which is 
    then validated and prepared for loading into the database.
- **Implementation Considerations**:
  - Parse the API response and convert it into the JSON format.
  - Validate the data structure to ensure it matches the database schema requirements.
  - If necessary, clean and normalize the data during this step.

### 3. Loading
- **Description**: This step involves inserting the structured data into the PostgreSQL database. 
    Efficient loading strategies, such as batch inserts, are implemented to improve performance.
- **Implementation Considerations**:
  - Use the `psycopg2` library for interaction with PostgreSQL.
  - Create database transactions to ensure data integrity.
  - Handle large data volumes using batch inserts and efficient database operations.

### 4. Database Design
- **Description**: The database schema is designed to store blockchain data (blocks, transactions, 
    addresses, etc.) in an efficient and normalized way.
- **Implementation Considerations**:
  - Define tables for key entities like blocks, transactions, and addresses.
  - Normalize the schema to avoid redundancy while maintaining performance.
  - Add indexes to frequently queried fields to speed up lookups.

### 5. Error Handling and Logging
- **Description**: Robust error handling and logging mechanisms ensure that the indexer can 
    gracefully handle errors and recover from them. The logs also help track the status of the data 
    extraction and processing.
- **Implementation Considerations**:
  - Use Python’s `logging` module to capture key events, errors, and performance metrics.
  - Implement retry logic for API failures and database errors.
  - Ensure that any unhandled exceptions are properly logged and investigated.

### 6. Testing
- **Description**: Develop tests to ensure the functionality of each component in the indexer. Both
    unit and integration tests are important  to verify correct behavior at different levels.
- **Implementation Considerations**:
  - Write unit tests for key functions (e.g., fetching data from the API, transforming it into JSON,
    and inserting it into the database).
  - Use integration tests to ensure the system works end-to-end (from data extraction to database 
    storage).
  - Implement mocks for external dependencies like API calls and database connections to isolate 
    test cases.

### 7. Documentation
- **Description**: Proper documentation ensures the project is easy to understand and maintain. The
    README and in-code documentation (docstrings) help developers understand how to set up, run, and
    modify the indexer.
- **Implementation Considerations**:
  - Write clear docstrings for all functions.
  - Document the overall architecture and flow of the application.
  - Provide a clear explanation of how to set up the environment, run the indexer, and troubleshoot 
    issues.

---

## How to Run the Project

### Prerequisites
1. **Python 3.8+** installed on your machine.
2. **PostgreSQL** installed and running.
3. **Redis** installed and running.
4. **Required Python libraries**:
    ```bash
    pip install -r requirements.txt
    ```

### Steps
1. Clone the repository:
    ```bash
    git clone <repository-url>
    cd blockchain-indexer
    ```

2. Set up the PostgreSQL database using the provided schema:
    ```bash
    psql -U <username> -d blockchain -f schema.sql
    ```

3. Run the indexer:
    ```bash
    python3 main.py
    ```

### Configuration
- Adjust the `config.py` file to match your environment for database and API settings.

---

## Future Improvements

1. **Performance Optimization**: 
   - Introduce parallel or asynchronous data fetching using libraries like `asyncio` or `aiohttp`.
   - Implement caching strategies to minimize duplicate API calls.

2. **Advanced Error Handling**: 
   - Build more sophisticated retry strategies for different types of API errors (rate limits, 
     server downtime).
   - Consider implementing circuit breakers or backoff strategies for repeated failures.

3. **Metrics and Monitoring**:
   - Add real-time metrics and monitoring to track system performance and health.
   - Integrate with monitoring tools like Prometheus or Grafana for visual insights.

4. **Blockchain Compatibility**: 
   - Extend the indexer to support multiple blockchain networks and APIs.
   - Create abstraction layers for handling different blockchain structures.

---

## Conclusion

This project presents a modular approach to designing
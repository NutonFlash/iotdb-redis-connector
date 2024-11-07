# Redis to IoTDB Data Migration Tool

A high-performance Java application designed for real-time data migration from Redis to Apache IoTDB. The tool efficiently schedules asynchronous fetch requests, processes data through a common queue, and utilizes a pool of writer threads to insert data points into IoTDB.

## Features

- Asynchronous data fetching from Redis
- Multi-threaded data writing to IoTDB
- Configurable batch sizes for optimal performance
- Automatic schema validation and creation
- Robust error handling and retry mechanism
- Comprehensive logging system
- Connection pooling for IoTDB
- CSV-based tag configuration

## Prerequisites

- Java 8 or higher
- Maven 3.x
- Access to Redis source system
- Apache IoTDB instance

## Configuration

The application is configured through a `config.json` file in the root directory. Here's the structure:

```json
{
    "source": {
        "redis": {
            "api_url": "http://127.0.0.1:20802/api/data/current.do",
            "user_key": "your_user_key"
        },
        "tags_file": "tagList.csv"
    },
    "destination": {
        "iotdb": {
            "host": "localhost",
            "port": 6667,
            "username": "root",
            "password": "root",
            "session_pool_size": 10
        }
    },
    "processing": {
        "fetcher": {
            "interval_ms": 1000,
            "timeout_ms": 10000
        },
        "writer": {
            "pool_size": 5,
            "batch_size": 500
        },
        "queue": {
            "capacity": 10000
        }
    },
    "retry": {
        "initial_delay_ms": 1000,
        "max_delay_ms": 60000,
        "max_attempts": 5,
        "backoff_multiplier": 2.0
    }
}
```

### Configuration Parameters

#### Source Configuration
- `redis.api_url`: Redis API endpoint URL
- `redis.user_key`: Authentication key for Redis
- `tags_file`: Path to CSV file containing tag definitions

#### Destination Configuration
- `iotdb`: IoTDB connection settings
- `session_pool_size`: Number of IoTDB sessions to maintain in the pool

#### Processing Configuration
- `fetcher.interval_ms`: Interval for fetching data from Redis
- `fetcher.timeout_ms`: Timeout for fetch requests
- `writer.pool_size`: Number of concurrent writer threads
- `writer.batch_size`: Batch size for writing to IoTDB
- `queue.capacity`: Size of the internal data queue

#### Retry Configuration
- `initial_delay_ms`: Initial retry delay
- `max_delay_ms`: Maximum retry delay
- `max_attempts`: Maximum number of retry attempts
- `backoff_multiplier`: Exponential backoff multiplier

## Building the Project
```bash
mvn clean package
```

## Running the Application
```bash
java -jar target/iotdb-redis-connector-1.0-SNAPSHOT.jar
```

## Logging

The application uses Logback for logging with the following configuration:

- Console logging for all levels
- File-based logging with hourly rotation
- Separate error log file
- Logs are stored in the `logs/` directory

## Error Handling

- Failed writes are logged to a separate file for tracking
- Automatic retry mechanism with exponential backoff
- Comprehensive error logging and reporting

## Performance Tuning

Adjust the following parameters in `config.json` for optimal performance:

1. Increase `writer.pool_size` for more parallelism
2. Adjust `fetcher.interval_ms` and `writer.batch_size` based on your data characteristics
3. Modify `queue.capacity` based on memory availability
4. Configure `session_pool_size` based on IoTDB server capacity
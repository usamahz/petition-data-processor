# Petition Data Processor

## Overview
This project processes government petition data using PySpark, extracting the top 20 most common words and creating a structured CSV output.

## Features
- Processes JSON input containing petition data
- Identifies top 20 most common words (5+ letters)
- Generates unique petition IDs
- Creates CSV output with word frequency analysis
- Includes comprehensive testing suite

## Requirements
- Python 3.9+
- Docker
- Docker Compose

## Setup
1. Clone the repository
2. Place input JSON file in `data/input/`
3. Run using Docker:

```bash
docker-compose build
docker-compose run pyspark
```



## Testing
Run tests using:
```bash
docker-compose run pyspark pytest
```


## Project Structure
- `src/`: Source code
  - `data_processing/`: Main processing logic
  - `utils/`: Utility functions
- `tests/`: Test suite
- `data/`: Input/output directories
- `docker/`: Docker configuration

## Output Format
The output CSV contains:
- petition_id: Unique identifier for each petition (monotonically increasing ID)
- Columns for each of the top 20 words with their frequencies

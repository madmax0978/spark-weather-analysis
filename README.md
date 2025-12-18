# Weather Analysis with Apache Spark

Big Data Project - ECE Paris 2025

## Overview

Collects weather data from 15 European cities via OpenWeather API and analyzes it with Apache Spark in Docker.

**What it does:**
- Fetches current weather for 15 cities
- Calculates statistics (avg, min, max temperatures)
- Ranks cities by temperature and humidity
- Computes correlations

## Quick Start

1. Get API key from https://openweathermap.org/api

2. Configure:
```bash
cp .env.example .env
# Edit .env with your API key
```

3. Collect data:
```bash
pip install -r config/requirements.txt
python scripts/collect_weather.py
```

4. Run Spark analysis:
```bash
docker-compose up -d
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/scripts/analyze_weather.py
```

5. View results:
```bash
cat data/output/global_stats/*.csv
```

## Structure

```
scripts/collect_weather.py    # Gets data from API
scripts/analyze_weather.py    # Spark analysis
docker-compose.yml            # Spark cluster
data/input/                   # Raw data
data/output/                  # Results
```

## Challenges

- API rate limiting: added sleep(1) between calls
- Docker volumes: forgot to mount data folder
- Spark output: used coalesce(1) for single files

## Technologies

- Apache Spark 3.5.0
- Docker & Docker Compose
- Python 3.11
- OpenWeather API

---

ECE Paris - January 2025

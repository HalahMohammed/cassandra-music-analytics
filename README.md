# cassandra-music-analytics
A data engineering project for modeling music streaming data with Apache Cassandra.

## Project Overview

Sparkify wants to analyze user activity on their music streaming app. This project creates an Apache Cassandra database optimized for specific queries about song plays and user behavior.

## Business Questions

1. **Session Playlist**: What song was played at a specific position in a user's session?
2. **User Session History**: What songs did a specific user listen to during a particular session?
3. **Song Listeners**: Which users have listened to a specific song?

## Technology Stack

- Apache Cassandra
- Python 3.7+
- Jupyter Notebooks

## Project Structure

- `src/etl_pipeline.py` - Processes raw event data into consolidated CSV
- `src/data_modeling.py` - Cassandra table creation and data loading
- `src/queries.py` - Business query executions
- `data/` - Raw and processed data files


